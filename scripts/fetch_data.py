#!/usr/bin/env python3
"""
ETL-скрипт для сбора данных маркетплейса по API.

Поддерживает три режима работы:
1. backfill - историческая загрузка за период (первичное заполнение БД)
2. daily - ежедневное обновление за вчерашний день (для cron)
3. --date YYYY-MM-DD - загрузка за конкретную дату

Использование:
    python scripts/fetch_data.py backfill
    python scripts/fetch_data.py daily
    python scripts/fetch_data.py --date 2023-06-15
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional

import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Загрузка переменных окружения из .env (ищет файл на уровень выше скрипта)
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

# Настройка логирования
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
log_dir = Path(__file__).parent.parent / 'logs'
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_dir / 'etl.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# Константы
API_URL = os.getenv('API_URL', 'http://final-project.simulative.ru/data/')
REQUEST_TIMEOUT = 15  # секунд
REQUEST_DELAY = 0.3   # задержка между запросами (секунд)
MAX_RETRIES = 3       # максимальное число попыток при ошибке сети


class DatabaseConnection:
    """Контекстный менеджер для безопасного подключения к БД с управлением транзакциями."""
    
    def __init__(self):
        self.config = {
            'dbname': os.getenv('POSTGRES_DB', 'marketplace'),
            'user': os.getenv('POSTGRES_USER', 'analyst'),
            'password': os.getenv('POSTGRES_PASSWORD', 'strong_password_123'),
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
        }
        self.conn = None
    
    def __enter__(self):
        self.conn = psycopg2.connect(**self.config)
        self.conn.autocommit = False
        return self.conn
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            if exc_type is None:
                self.conn.commit()
            else:
                self.conn.rollback()
                logger.error(f"Транзакция отменена из-за ошибки: {exc_val}")
            self.conn.close()


class APIClient:
    """Клиент для работы с API маркетплейса с retry-логикой и обработкой редиректов."""
    
    def __init__(self, base_url: str, timeout: int = REQUEST_TIMEOUT, delay: float = REQUEST_DELAY):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'DiplomaETL/1.0'
        })
    
    def fetch_day(self, target_date: datetime) -> Optional[List[Dict]]:
        """
        Получает данные за конкретный день с retry-логикой.
        """
        date_str = target_date.strftime('%Y-%m-%d')
        url = f"{self.base_url}/?date={date_str}"
        
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                time.sleep(self.delay)
                response = self.session.get(url, timeout=self.timeout)
                
                if response.status_code == 200:
                    data = response.json()
                    logger.info(f"✓ {date_str}: получено {len(data)} записей")
                    return data
                elif response.status_code == 404:
                    logger.warning(f"⚠ {date_str}: данных нет (404)")
                    return []
                elif response.status_code in (301, 302):
                    # Обработка редиректа (добавляем слэш, если API его требует)
                    url_with_slash = url.replace('/data?', '/data/?')
                    response = self.session.get(url_with_slash, timeout=self.timeout)
                    if response.status_code == 200:
                        data = response.json()
                        logger.info(f"✓ {date_str} (redirect): получено {len(data)} записей")
                        return data
                else:
                    logger.warning(f"⚠ {date_str}: HTTP {response.status_code} (попытка {attempt}/{MAX_RETRIES})")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"✗ {date_str}: сетевая ошибка (попытка {attempt}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(2 ** attempt)  # экспоненциальная задержка
        
        logger.error(f"✗ {date_str}: не удалось получить данные после {MAX_RETRIES} попыток")
        return None


def validate_record(record: Dict) -> bool:
    """Валидация одной записи перед сохранением."""
    required_fields = ['client_id', 'purchase_datetime', 'product_id']
    if not all(field in record for field in required_fields):
        return False
    if record.get('total_price', -1) < 0:
        return False
    return True


def save_records(conn, records: List[Dict]) -> int:
    """Сохраняет список записей в БД с валидацией и идемпотентностью."""
    if not records:
        return 0
    
    valid_records = [r for r in records if validate_record(r)]
    skipped = len(records) - len(valid_records)
    if skipped > 0:
        logger.warning(f"Пропущено {skipped} невалидных записей")
    
    if not valid_records:
        return 0
    
    tuples = [
        (
            r['client_id'],
            r.get('gender'),
            r['purchase_datetime'],
            r.get('purchase_time_as_seconds_from_midnight', 0),
            r['product_id'],
            r.get('quantity', 0),
            r.get('price_per_item', 0),
            r.get('discount_per_item', 0),
            r.get('total_price', 0)
        )
        for r in valid_records
    ]
    
    query = """
        INSERT INTO sales (
            client_id, gender, purchase_datetime, purchase_time_seconds,
            product_id, quantity, price_per_item, discount_per_item, total_price
        ) VALUES %s
        ON CONFLICT (client_id, purchase_datetime, product_id, quantity, total_price) 
        DO NOTHING
    """
    
    with conn.cursor() as cursor:
        execute_values(cursor, query, tuples)
        return cursor.rowcount


def backfill(start_date: str, end_date: str) -> None:
    """Историческая загрузка данных за период."""
    logger.info(f"🚀 Начало исторической загрузки: {start_date} → {end_date}")
    
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    client = APIClient(API_URL)
    total_inserted = 0
    total_days = (end - start).days + 1
    
    with DatabaseConnection() as conn:
        for day_offset in range(total_days):
            current_date = start + timedelta(days=day_offset)
            date_str = current_date.strftime('%Y-%m-%d')
            
            logger.info(f"[{day_offset + 1}/{total_days}] Обработка {date_str}")
            
            records = client.fetch_day(current_date)
            if records is None:
                logger.error(f"Пропуск {date_str} из-за ошибки")
                continue
            
            inserted = save_records(conn, records)
            total_inserted += inserted
            logger.info(f"  → Вставлено: {inserted}")
    
    logger.info(f"✅ Историческая загрузка завершена. Всего добавлено: {total_inserted:,} записей")


def fetch_daily() -> None:
    """Ежедневное обновление: загрузка данных за вчерашний день."""
    yesterday = datetime.now() - timedelta(days=1)
    date_str = yesterday.strftime('%Y-%m-%d')
    
    logger.info(f"📥 Ежедневное обновление: загрузка за {date_str}")
    
    client = APIClient(API_URL)
    records = client.fetch_day(yesterday)
    
    if records is None:
        logger.error("Ежедневное обновление завершено с ошибкой")
        return
    
    with DatabaseConnection() as conn:
        inserted = save_records(conn, records)
    
    logger.info(f"✅ Ежедневное обновление завершено. Добавлено: {inserted:,} записей")


def main():
    """Точка входа в скрипт."""
    if len(sys.argv) > 1:
        if sys.argv[1] == 'backfill':
            start = os.getenv('API_BACKFILL_START', '2023-01-01')
            end = os.getenv('API_BACKFILL_END', '2023-12-31')
            backfill(start, end)
        elif sys.argv[1] == 'daily':
            fetch_daily()
        elif sys.argv[1] == '--date' and len(sys.argv) == 3:
            target = datetime.strptime(sys.argv[2], '%Y-%m-%d')
            client = APIClient(API_URL)
            records = client.fetch_day(target)
            if records is not None:
                with DatabaseConnection() as conn:
                    inserted = save_records(conn, records)
                logger.info(f"Загрузка за {sys.argv[2]}: {inserted} записей")
        else:
            print(__doc__)
            sys.exit(1)
    else:
        fetch_daily()


if __name__ == '__main__':
    main()
