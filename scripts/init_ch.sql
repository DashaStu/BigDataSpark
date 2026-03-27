CREATE DATABASE IF NOT EXISTS reports;

-- 1. Витрина продуктов
CREATE TABLE IF NOT EXISTS reports.rep1_products (name String, category String, revenue Float64, sales_count Int64, avg_rating Float64, reviews_count Int64) ENGINE = Log;
-- 2. Витрина клиентов
CREATE TABLE IF NOT EXISTS reports.rep2_customers (name String, country String, total_spent Float64, avg_check Float64) ENGINE = Log;
-- 3. Витрина времени
CREATE TABLE IF NOT EXISTS reports.rep3_time (year Int32, month Int32, total_revenue Float64, avg_order_size Float64) ENGINE = Log;
-- 4. Витрина магазинов
CREATE TABLE IF NOT EXISTS reports.rep4_stores (name String, city String, country String, revenue Float64, avg_check Float64) ENGINE = Log;
-- 5. Витрина поставщиков
CREATE TABLE IF NOT EXISTS reports.rep5_suppliers (name String, country String, revenue Float64, avg_item_price Float64) ENGINE = Log;
-- 6. Витрина качества
CREATE TABLE IF NOT EXISTS reports.rep6_quality (name String, avg_rating Float64, sales_volume Int64, reviews_count Int64) ENGINE = Log;