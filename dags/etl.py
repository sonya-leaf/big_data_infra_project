from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from clickhouse_driver import Client
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import concurrent.futures
import os
import re
import math
import random
import time
import logging
import gc
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DELAYS = {
    'category': (0.3, 0.7),
    'page': (0.3, 0.7),
    'product': (0.5, 1.0),
}

MAX_WORKERS = 5
BATCH_SIZE = 100

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_clickhouse_client():
    return Client(
        host=os.environ['CLICKHOUSE_HOST'],
        port=int(os.environ['CLICKHOUSE_PORT']),
        user=os.environ['CLICKHOUSE_USER'],
        password=os.environ['CLICKHOUSE_PASSWORD']
    )


def create_session():
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
    })
    
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=5, pool_maxsize=5)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    session.verify = False
    session.timeout = (10, 30)
    
    return session


def filter_categories(categories):
    exclude_keywords = [
        'новинки', 'хиты', 'торты на заказ', 'детский праздник', 'кейтеринг',
        'косметика', 'средства гигиены', 'товары для дома', 'товары для животных',
        'здоровье', 'подарочные карты', 'добрая полка', 'доставка по россии',
        'идеи для подарков'
    ]

    filtered_categories = []

    for category in categories:
        category_name = category['name'].lower().replace('\xa0', ' ')

        should_exclude = False
        for keyword in exclude_keywords:
            if keyword in category_name:
                should_exclude = True
                break

        if 'vkusvill.ruhttps://' in category['url']:
            should_exclude = True

        if '/goods/' not in category['url']:
            should_exclude = True

        if not should_exclude:
            filtered_categories.append(category)

    logger.info(f"Filtered categories: {len(filtered_categories)} from {len(categories)}")
    return filtered_categories


def extract_categories(base_url, session):
    logger.info("Extracting categories...")
    time.sleep(random.uniform(*DELAYS['category']))

    response = session.get(base_url + '/goods/', timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')

    categories = []
    menu = soup.find('div', class_='VVCatalog2020Menu')
    if menu:
        items = menu.find_all('li', class_='VVCatalog2020Menu__Item')
        for item in items:
            link_tag = item.find('a', class_='VVCatalog2020Menu__Link')
            if link_tag and link_tag.get('href'):
                category_name = link_tag.find('span', class_='_text')
                categories.append({
                    'name': category_name.get_text(strip=True) if category_name else None,
                    'url': base_url + link_tag['href']
                })
    
    del soup
    del response
    gc.collect()

    logger.info(f"Found {len(categories)} categories")
    return categories


def get_total_products_count(soup):
    total_input = soup.find('input', id='js-catalog-page-param-total-products')
    if total_input and total_input.get('value'):
        try:
            return int(total_input['value'])
        except ValueError:
            pass
    return None


def calculate_max_pages(total_products, items_per_page=24):
    if total_products:
        return math.ceil(total_products / items_per_page)
    return 1


def extract_product_links_from_page(soup, base_url='https://vkusvill.ru'):
    product_links = []
    product_cards = soup.find_all('div', class_='ProductCard__content')
    for card in product_cards:
        link_tag = card.find('a', class_='ProductCard__link')
        if link_tag and link_tag.get('href'):
            href = link_tag['href']
            if not href.startswith('http'):
                href = base_url + href
            product_links.append(href)
    return product_links


def extract_product_links(category_url, session):
    all_product_links = []

    logger.info(f"Extracting product links from {category_url}")
    time.sleep(random.uniform(*DELAYS['page']))
    response = session.get(category_url, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')

    product_links = extract_product_links_from_page(soup)
    all_product_links.extend(product_links)

    total_products = get_total_products_count(soup)

    if total_products:
        logger.info(f"Total products: {total_products}, pages: {calculate_max_pages(total_products)}")
        max_pages = calculate_max_pages(total_products)
    else:
        max_pages = 1

    del soup
    del response
    gc.collect()

    if max_pages > 1:
        for page_num in range(2, max_pages + 1):
            time.sleep(random.uniform(*DELAYS['page']))

            if '?' in category_url:
                page_url = f"{category_url}&PAGEN_1={page_num}"
            else:
                page_url = f"{category_url}?PAGEN_1={page_num}"

            try:
                response = session.get(page_url, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')

                product_links = extract_product_links_from_page(soup)
                all_product_links.extend(product_links)
                
                del soup
                del response
                gc.collect()

                if not product_links:
                    break

            except Exception as e:
                logger.warning(f"Error on page {page_num}: {e}")
                continue

    all_product_links = list(set(all_product_links))
    logger.info(f"Found {len(all_product_links)} unique product links")

    return all_product_links


def extract_price_info(soup):
    price = None
    price_unit = None

    price_meta = soup.find('meta', itemprop='price')
    if price_meta:
        price = price_meta.get('content')

    price_element = soup.find('span', class_='Price')
    if price_element:
        price_text = price_element.get_text(strip=True)
        match = re.search(r'/(\S+)', price_text)
        if match:
            price_unit = match.group(1)

    if not price_unit:
        price_unit = 'шт'

    return price, price_unit


def parse_price(price_str):
    if not price_str:
        return None
    try:
        return float(str(price_str).replace(' ', '').replace(',', '.'))
    except:
        return None


def safe_float(value):
    if value is None:
        return None
    value = str(value).strip()
    if value == "" or value == "Ждёт оценку" or value == "None":
        return None
    try:
        return float(value.replace(",", "."))
    except:
        return None


def extract_product_info(url, session):
    for attempt in range(3):
        try:
            time.sleep(random.uniform(*DELAYS['product']))
            response = session.get(url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            title = None
            title_meta = soup.find('div', class_='hidden', itemprop='name')
            if title_meta:
                title = title_meta.get_text(strip=True)
            else:
                title_tag = soup.find('h1')
                title = title_tag.get_text(strip=True) if title_tag else None

            description = None
            desc_meta = soup.find('div', class_='hidden', itemprop='description')
            if desc_meta:
                description = desc_meta.get_text(strip=True)

            category = None
            subcategory = None
            breadcrumbs = soup.find_all('span', class_='Breadcrumbs__link', itemprop='itemListElement')

            if len(breadcrumbs) >= 2:
                categories_list = []
                for crumb in breadcrumbs:
                    name_tag = crumb.find('span', itemprop='name')
                    if name_tag:
                        categories_list.append(name_tag.get_text(strip=True))

                if len(categories_list) >= 2:
                    category = categories_list[-2]
                    subcategory = categories_list[-1]

                    if subcategory == title and len(categories_list) >= 3:
                        category = categories_list[-3]
                        subcategory = categories_list[-2]

            price, price_unit = extract_price_info(soup)
            price_float = parse_price(price)

            rating = None
            rating_tag = soup.find('div', class_='Rating__text', id='js-product-api-reviews-rate-value')
            if rating_tag:
                rating = rating_tag.get_text(strip=True)

            nutrition = {}
            energy_items = soup.find_all('div', class_='VV23_DetailProdPageAccordion__EnergyItem')
            for item in energy_items:
                value_tag = item.find('div', class_='VV23_DetailProdPageAccordion__EnergyValue')
                desc_tag = item.find('div', class_='VV23_DetailProdPageAccordion__EnergyDesc')
                if value_tag and desc_tag:
                    key = desc_tag.get_text(strip=True)
                    value = value_tag.get_text(strip=True)
                    nutrition[key] = value

            shelf_life = brand = storage_conditions = manufacturer = country = composition = weight = None

            info_items = soup.find_all('div', class_='VV23_DetailProdPageInfoDescItem')
            for item in info_items:
                title_tag = item.find('h4', class_='VV23_DetailProdPageInfoDescItem__Title')
                if not title_tag:
                    continue

                title_text = title_tag.get_text(strip=True)
                desc_tag = item.find('div', class_='VV23_DetailProdPageInfoDescItem__Desc')

                if desc_tag:
                    desc_text = desc_tag.get_text(strip=True)

                    if 'Годен' in title_text:
                        shelf_life = desc_text
                    elif 'Бренд' in title_text:
                        brand = desc_text
                    elif 'Условия хранения' in title_text:
                        storage_conditions = desc_text
                    elif 'Изготовитель' in title_text:
                        manufacturer = desc_text
                    elif 'Страна производства' in title_text:
                        country = desc_text
                    elif 'Состав' in title_text:
                        composition = desc_text
                    elif 'Вес' in title_text or 'Объем' in title_text:
                        weight = desc_text

            labels = []
            label_tags = soup.find_all('div', class_='ProductCardLabel')
            for label in label_tags:
                label_text = label.get('title')
                if label_text:
                    labels.append(label_text)

            result = {
                'title': title,
                'category': category,
                'subcategory': subcategory,
                'price': price_float,
                'price_unit': price_unit if price_unit else 'шт',
                'weight': weight,
                'rating': rating,
                'description': description,
                'calories': nutrition.get('Ккал'),
                'proteins': nutrition.get('Белки, г'),
                'fats': nutrition.get('Жиры, г'),
                'carbs': nutrition.get('Углеводы, г'),
                'shelf_life': shelf_life,
                'brand': brand,
                'storage_conditions': storage_conditions,
                'manufacturer': manufacturer,
                'country': country,
                'composition': composition,
                'labels': ', '.join(labels) if labels else None,
                'url': url,
                'parsed_at': datetime.now()
            }
            
            del soup
            del response
            gc.collect()
            
            return result
            
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < 2:
                time.sleep(5)
            else:
                return None
    return None


def save_batch_to_clickhouse(batch, client, db):
    if not batch:
        return
    
    try:
        values = []
        for record in batch:
            row = (
                record['title'] if record['title'] is not None else '',
                record['category'] if record['category'] is not None else '',
                record['subcategory'] if record['subcategory'] is not None else '',
                int(record['price']) if record['price'] is not None else 0,
                record['price_unit'] if record['price_unit'] is not None else 'шт',
                record['weight'],
                safe_float(record['rating']),
                record['description'],
                safe_float(record['calories']),
                safe_float(record['proteins']),
                safe_float(record['fats']),
                safe_float(record['carbs']),
                record['shelf_life'],
                record['brand'],
                record['storage_conditions'],
                record['manufacturer'],
                record['country'],
                record['composition'],
                record['labels'],
                record['url'] if record['url'] is not None else '',  # <--- вот тут!
                record['parsed_at']
            )
            values.append(row)
        
        client.execute(f'INSERT INTO {db}.products VALUES', values)
        logger.info(f"Saved batch of {len(batch)} records to ClickHouse")
    except Exception as e:
        logger.error(f"Error saving batch: {e}")
        if batch:
            logger.error(f"Problem record: {batch[0]}")
            raise


def process_product_and_save(url, session, client, db):
    try:
        result = extract_product_info(url, session)
        if result:
            return result
        return None
    except Exception as e:
        logger.error(f"Failed to process {url}: {e}")
        return None


def create_table():
    logger.info("Creating table if not exists...")
    client = get_clickhouse_client()
    db = os.environ['CLICKHOUSE_DB']
    client.execute(f"""
        CREATE TABLE IF NOT EXISTS {db}.products (
            title String,
            category String,
            subcategory String,
            price UInt32,
            price_unit LowCardinality(String),
            weight Nullable(String),
            rating Nullable(Float32),
            description Nullable(String),
            calories Nullable(Float32),
            proteins Nullable(Float32),
            fats Nullable(Float32),
            carbs Nullable(Float32),
            shelf_life Nullable(String),
            brand LowCardinality(String),
            storage_conditions Nullable(String),
            manufacturer Nullable(String),
            country Nullable(String),
            composition Nullable(String),
            labels Nullable(String),
            url String,
            parsed_at DateTime
        ) ENGINE = MergeTree()
        ORDER BY (parsed_at, category, subcategory, title)
    """)
    logger.info("Table ready")


def scrape(**context):
    logger.info("=" * 60)
    logger.info("Starting scrape process")
    logger.info("=" * 60)

    base_url = 'https://vkusvill.ru'
    session = create_session()
    client = get_clickhouse_client()
    db = os.environ['CLICKHOUSE_DB']
    
    total_saved = 0
    batch = []

    logger.info("Getting categories...")
    categories = extract_categories(base_url, session)
    logger.info(f"Found {len(categories)} categories")

    categories = filter_categories(categories)
    logger.info(f"After filtering: {len(categories)} categories")

    total_categories = len(categories)

    for idx, category in enumerate(categories, 1):
        logger.info(f"[{idx}/{total_categories}] Processing category: {category['name']}")
        logger.info(f"URL: {category['url']}")
        logger.info("-" * 60)

        try:
            product_links = extract_product_links(category['url'], session)

            if not product_links:
                logger.warning(f"No products found in category {category['name']}")
                continue

            logger.info(f"Parsing {len(product_links)} products from {category['name']}")
            successful = 0

            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(process_product_and_save, url, session, client, db): url for url in product_links}

                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    if result:
                        batch.append(result)
                        successful += 1
                        total_saved += 1

                        if len(batch) >= BATCH_SIZE:
                            save_batch_to_clickhouse(batch, client, db)
                            batch = []
                            gc.collect()

                        if successful % 50 == 0:
                            logger.info(f"Progress: {successful}/{len(product_links)} products parsed")
                            logger.info(f"Total saved so far: {total_saved}")

            if batch:
                save_batch_to_clickhouse(batch, client, db)
                batch = []
                gc.collect()

            logger.info(f"Category {category['name']} completed: {successful}/{len(product_links)} products")

        except Exception as e:
            logger.error(f"Error in category {category['name']}: {e}")
            continue
        
        gc.collect()

    if batch:
        save_batch_to_clickhouse(batch, client, db)

    logger.info(f"Scrape completed. Total products saved: {total_saved}")
    context['ti'].xcom_push(key='total_saved', value=total_saved)


def show_stats(**context):
    logger.info("=" * 60)
    logger.info("CLICKHOUSE STATISTICS")
    logger.info("=" * 60)
    
    client = get_clickhouse_client()
    db = os.environ['CLICKHOUSE_DB']
    
    total_saved = context['ti'].xcom_pull(key='total_saved', task_ids='scrape')
    
    try:
        total_count = client.execute(f"SELECT count(*) FROM {db}.products")[0][0]
        logger.info(f"Total records in table: {total_count}")
        
        if total_saved:
            logger.info(f"Records added in this run: {total_saved}")
        
        logger.info("-" * 60)
        
        date_range = client.execute(f"""
            SELECT 
                min(parsed_at) as min_parsed_at,
                max(parsed_at) as max_parsed_at
            FROM {db}.products
        """)[0]
        logger.info(f"Date range: {date_range[0]} to {date_range[1]}")
        
        logger.info("-" * 60)
        
        categories_stats = client.execute(f"""
            SELECT 
                category,
                count(*) as count
            FROM {db}.products
            WHERE category IS NOT NULL
            GROUP BY category
            ORDER BY count DESC
            LIMIT 10
        """)
        
        logger.info("Top 10 categories by product count:")
        for cat, count in categories_stats:
            logger.info(f"  {cat}: {count} products")
        
        logger.info("-" * 60)
        
        sample = client.execute(f"""
            SELECT 
                title,
                category,
                price,
                rating,
                url
            FROM {db}.products
            ORDER BY parsed_at DESC
            LIMIT 5
        """)
        
        logger.info("Latest 5 products added:")
        for row in sample:
            logger.info(f"  {row[0]} | {row[1]} | {row[2]}₽ | {row[3]}★")
            logger.info(f"    {row[4]}")
        
        logger.info("-" * 60)
        
        price_stats = client.execute(f"""
            SELECT 
                avg(price) as avg_price,
                min(price) as min_price,
                max(price) as max_price
            FROM {db}.products
            WHERE price IS NOT NULL
        """)[0]
        logger.info(f"Price stats: avg={price_stats[0]:.2f}₽, min={price_stats[1]:.2f}₽, max={price_stats[2]:.2f}₽")
        
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Error getting stats: {e}")


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
    schedule_interval='0 8 * * *',
    catchup=False,
    max_active_runs=1
) as dag:
    create = PythonOperator(task_id='create', python_callable=create_table)
    scrape_task = PythonOperator(task_id='scrape', python_callable=scrape)
    stats_task = PythonOperator(task_id='stats', python_callable=show_stats)
    create >> scrape_task >> stats_task