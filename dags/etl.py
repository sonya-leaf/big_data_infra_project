from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from clickhouse_driver import Client
import requests
from bs4 import BeautifulSoup
import concurrent.futures
import os
import re
import math
import random
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DELAYS = {'category': (0.3, 0.7), 'page': (0.3, 0.7), 'product': (0.1, 0.3)}
MAX_WORKERS = 10


def create_session():
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
    })
    session.timeout = 30
    return session


def filter_categories(categories):
    exclude_keywords = ['новинки', 'хиты', 'торты на заказ', 'детский праздник', 'кейтеринг',
                        'косметика', 'средства гигиены', 'товары для дома', 'товары для животных',
                        'здоровье', 'подарочные карты', 'добрая полка', 'доставка по россии', 'идеи для подарков']
    filtered = [c for c in categories 
                if not any(kw in c['name'].lower().replace('\xa0', ' ') for kw in exclude_keywords)
                and '/goods/' in c['url']
                and 'vkusvill.ruhttps://' not in c['url']]
    logger.info(f"Filtered categories: {len(filtered)} from {len(categories)}")
    return filtered


def extract_categories(base_url, session):
    logger.info("Extracting categories...")
    time.sleep(random.uniform(*DELAYS['category']))
    response = session.get(base_url + '/goods/')
    response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')
    categories = []
    menu = soup.find('div', class_='VVCatalog2020Menu')
    if menu:
        for item in menu.find_all('li', class_='VVCatalog2020Menu__Item'):
            link = item.find('a', class_='VVCatalog2020Menu__Link')
            if link and link.get('href'):
                name = link.find('span', class_='_text')
                categories.append({
                    'name': name.get_text(strip=True) if name else None,
                    'url': base_url + link['href']
                })
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


def extract_product_links(category_url, session):
    logger.debug(f"Extracting product links from {category_url}")
    all_product_links = []
    time.sleep(random.uniform(*DELAYS['page']))
    response = session.get(category_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')
    
    for card in soup.find_all('div', class_='ProductCard__content'):
        link = card.find('a', class_='ProductCard__link')
        if link and link.get('href'):
            href = link['href']
            if not href.startswith('http'):
                href = 'https://vkusvill.ru' + href
            all_product_links.append(href)
    
    total_products = get_total_products_count(soup)
    max_pages = math.ceil(total_products / 24) if total_products else 1
    logger.debug(f"Total products: {total_products}, pages: {max_pages}")
    
    for page_num in range(2, max_pages + 1):
        time.sleep(random.uniform(*DELAYS['page']))
        page_url = f"{category_url}?PAGEN_1={page_num}" if '?' not in category_url else f"{category_url}&PAGEN_1={page_num}"
        try:
            response = session.get(page_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            for card in soup.find_all('div', class_='ProductCard__content'):
                link = card.find('a', class_='ProductCard__link')
                if link and link.get('href'):
                    href = link['href']
                    if not href.startswith('http'):
                        href = 'https://vkusvill.ru' + href
                    all_product_links.append(href)
            if not soup.find_all('div', class_='ProductCard__content'):
                logger.debug(f"No products on page {page_num}, stopping")
                break
        except Exception as e:
            logger.warning(f"Error on page {page_num}: {e}")
            continue
    
    unique_links = list(set(all_product_links))
    logger.info(f"Found {len(unique_links)} unique product links")
    return unique_links


def extract_price_info(soup):
    price = None
    price_unit = 'шт'
    price_meta = soup.find('meta', itemprop='price')
    if price_meta:
        price = price_meta.get('content')
    price_element = soup.find('span', class_='Price')
    if price_element:
        price_text = price_element.get_text(strip=True)
        match = re.search(r'/(\S+)', price_text)
        if match:
            price_unit = match.group(1)
    return price, price_unit


def extract_product_info(url, session):
    time.sleep(random.uniform(*DELAYS['product']))
    response = session.get(url)
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
            nutrition[desc_tag.get_text(strip=True)] = value_tag.get_text(strip=True)
    
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
    
    return {
        'date': datetime.now().date(),
        'title': title,
        'category': category,
        'subcategory': subcategory,
        'price': float(price) if price else None,
        'price_unit': price_unit,
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


def extract_product_info_safe(url, session):
    try:
        return extract_product_info(url, session)
    except Exception as e:
        logger.error(f"Failed to extract product {url}: {e}")
        return None


def create_table():
    logger.info("Creating table if not exists...")
    client = Client(
        host=os.environ['CLICKHOUSE_HOST'],
        port=int(os.environ['CLICKHOUSE_PORT']),
        user=os.environ['CLICKHOUSE_USER'],
        password=os.environ['CLICKHOUSE_PASSWORD']
    )
    db = os.environ['CLICKHOUSE_DB']
    client.execute(f"""
        CREATE TABLE IF NOT EXISTS {db}.vkusvill_products (
            date Date,
            title String,
            category String,
            subcategory String,
            price Float64,
            price_unit String,
            weight String,
            rating String,
            description String,
            calories String,
            proteins String,
            fats String,
            carbs String,
            shelf_life String,
            brand String,
            storage_conditions String,
            manufacturer String,
            country String,
            composition String,
            labels String,
            url String,
            parsed_at DateTime
        ) ENGINE = MergeTree() ORDER BY date
    """)
    logger.info("Table ready")


def scrape(**context):
    logger.info("=" * 60)
    logger.info("Starting scrape process")
    logger.info("=" * 60)
    
    base_url = 'https://vkusvill.ru'
    session = create_session()
    
    categories = extract_categories(base_url, session)
    categories = filter_categories(categories)
    
    all_products = []
    total_categories = len(categories)
    
    for idx, category in enumerate(categories, 1):
        logger.info(f"[{idx}/{total_categories}] Processing category: {category['name']}")
        product_links = extract_product_links(category['url'], session)
        
        if not product_links:
            logger.warning(f"No products found in category {category['name']}")
            continue
        
        logger.info(f"Parsing {len(product_links)} products from {category['name']}")
        successful = 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(extract_product_info_safe, url, create_session()): url for url in product_links}
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result:
                    all_products.append(result)
                    successful += 1
                    
                    if successful % 50 == 0:
                        logger.info(f"Progress: {successful}/{len(product_links)} products parsed")
        
        logger.info(f"Category {category['name']} completed: {successful}/{len(product_links)} products")
    
    logger.info(f"Scrape completed. Total products collected: {len(all_products)}")
    context['ti'].xcom_push(key='data', value=all_products)


def save(**context):
    logger.info("Starting save process to ClickHouse")
    data = context['ti'].xcom_pull(key='data', task_ids='scrape')
    
    if not data:
        logger.warning("No data to save")
        return
    
    logger.info(f"Saving {len(data)} records to ClickHouse")
    
    client = Client(
        host=os.environ['CLICKHOUSE_HOST'],
        port=int(os.environ['CLICKHOUSE_PORT']),
        user=os.environ['CLICKHOUSE_USER'],
        password=os.environ['CLICKHOUSE_PASSWORD']
    )
    db = os.environ['CLICKHOUSE_DB']
    
    batch_size = 100
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        for record in batch:
            client.execute(f'INSERT INTO {db}.vkusvill_products VALUES', [tuple(record.values())])
        logger.info(f"Saved batch {i//batch_size + 1}/{(len(data)-1)//batch_size + 1}")
    
    logger.info(f"Save completed. {len(data)} records inserted")


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