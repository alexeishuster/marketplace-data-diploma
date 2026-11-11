-- Схема базы данных для дипломного проекта "Аналитика маркетплейса"
-- PostgreSQL 15

-- 1. Создание таблицы транзакций
CREATE TABLE IF NOT EXISTS sales (
    id SERIAL PRIMARY KEY,
    client_id INTEGER NOT NULL,
    gender VARCHAR(1) CHECK (gender IN ('M', 'F', NULL)),
    purchase_datetime DATE NOT NULL,
    purchase_time_seconds INTEGER CHECK (purchase_time_seconds >= 0 AND purchase_time_seconds < 86400),
    product_id INTEGER NOT NULL,
    quantity INTEGER CHECK (quantity >= 0),
    price_per_item NUMERIC(15, 2) CHECK (price_per_item >= 0),
    discount_per_item NUMERIC(15, 2) CHECK (discount_per_item >= 0),
    total_price NUMERIC(15, 2) CHECK (total_price >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Индексы для ускорения аналитических запросов (критично для 2M+ строк)
CREATE INDEX IF NOT EXISTS idx_sales_date ON sales(purchase_datetime);
CREATE INDEX IF NOT EXISTS idx_sales_client ON sales(client_id);
CREATE INDEX IF NOT EXISTS idx_sales_product ON sales(product_id);
CREATE INDEX IF NOT EXISTS idx_sales_gender ON sales(gender);
CREATE INDEX IF NOT EXISTS idx_sales_total_price ON sales(total_price);

-- 3. Уникальный индекс для предотвращения дубликатов при повторном запуске ETL (идемпотентность)
CREATE UNIQUE INDEX IF NOT EXISTS idx_sales_unique 
ON sales(client_id, purchase_datetime, product_id, quantity, total_price);

-- 4. Комментарии к таблице и колонкам (для удобства проверяющих в DBeaver/pgAdmin)
COMMENT ON TABLE sales IS 'Транзакции маркетплейса, загружаемые из API';
COMMENT ON COLUMN sales.client_id IS 'Идентификатор клиента';
COMMENT ON COLUMN sales.gender IS 'Пол клиента: M - мужской, F - женский';
COMMENT ON COLUMN sales.purchase_datetime IS 'Дата покупки';
COMMENT ON COLUMN sales.purchase_time_seconds IS 'Время покупки в секундах от полуночи';
COMMENT ON COLUMN sales.product_id IS 'Идентификатор товара';
COMMENT ON COLUMN sales.quantity IS 'Количество единиц товара';
COMMENT ON COLUMN sales.price_per_item IS 'Цена за единицу по прайсу';
COMMENT ON COLUMN sales.discount_per_item IS 'Скидка на единицу товара';
COMMENT ON COLUMN sales.total_price IS 'Итоговая сумма покупки (после скидки)';

-- 5. Представление (View) для быстрого просмотра дневной статистики
CREATE OR REPLACE VIEW v_daily_stats AS
SELECT 
    purchase_datetime AS day,
    COUNT(*) AS transactions,
    COUNT(DISTINCT client_id) AS unique_clients,
    ROUND(SUM(total_price), 2) AS revenue,
    ROUND(AVG(total_price), 2) AS avg_check,
    ROUND(100.0 * SUM(discount_per_item * quantity) / NULLIF(SUM(price_per_item * quantity), 0), 2) AS discount_rate_pct
FROM sales
WHERE total_price > 0
GROUP BY purchase_datetime
ORDER BY purchase_datetime;

COMMENT ON VIEW v_daily_stats IS 'Агрегированная дневная статистика продаж для быстрого мониторинга';
