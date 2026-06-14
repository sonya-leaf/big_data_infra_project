from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import random
import os
from clickhouse_driver import Client


def create_table():
    client = Client(
        host=os.environ['CLICKHOUSE_HOST'],
        port=int(os.environ['CLICKHOUSE_PORT']),
        user=os.environ['CLICKHOUSE_USER'],
        password=os.environ['CLICKHOUSE_PASSWORD']
    )
    db = os.environ['CLICKHOUSE_DB']
    client.execute(f"""CREATE TABLE IF NOT EXISTS {db}.items 
    (date Date, title String, amount UInt32, price Float64)
    ENGINE = MergeTree() ORDER BY date""")
    print("Table ready")


def scrape(**context):
    titles = ['Laptop', 'Phone', 'Headphones', 'Keyboard', 'Mouse']
    data = []
    for _ in range(random.randint(3, 8)):
        data.append({
            'date': datetime.now().date(),
            'title': random.choice(titles),
            'amount': random.randint(1, 5),
            'price': round(random.uniform(10, 500), 2)
        })
    context['ti'].xcom_push(key='data', value=data)
    print(f"Scraped {len(data)} items")


def save(**context):
    data = context['ti'].xcom_pull(key='data', task_ids='scrape')
    if data:
        client = Client(
            host=os.environ['CLICKHOUSE_HOST'],
            port=int(os.environ['CLICKHOUSE_PORT']),
            user=os.environ['CLICKHOUSE_USER'],
            password=os.environ['CLICKHOUSE_PASSWORD']
        )
        db = os.environ['CLICKHOUSE_DB']
        for record in data:
            client.execute(f'INSERT INTO {db}.sales VALUES', [tuple(record.values())])
        print(f"Saved {len(data)} records")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'etl',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    catchup=False,
    max_active_runs=1
) as dag:
    create = PythonOperator(task_id='create', python_callable=create_table)
    scrape_task = PythonOperator(task_id='scrape', python_callable=scrape)
    save_task = PythonOperator(task_id='save', python_callable=save)
    create >> scrape_task >> save_task
