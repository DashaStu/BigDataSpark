import urllib.request
import csv
import io
from base64 import b64encode
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Final_StarSchema_Reports").getOrCreate()
pg_url = "jdbc:postgresql://postgres:5432/main_db"
pg_props = {"user": "user", "password": "password", "driver": "org.postgresql.Driver"}


def send_to_ch(table, df):
    auth = f"Basic {b64encode(b'user:password').decode('ascii')}"
    headers = {"Authorization": auth}
    rows = df.collect()
    output = io.StringIO()
    writer = csv.writer(output, quoting=csv.QUOTE_NONNUMERIC)
    for r in rows: writer.writerow(list(r))
    csv_body = output.getvalue()
    url = f"http://clickhouse:8123/?query=INSERT+INTO+reports.{table}+FORMAT+CSV"
    urllib.request.urlopen(urllib.request.Request(url, data=csv_body.encode("utf-8"), headers=headers))


def run_etl():
    raw = spark.read.csv("/opt/bitnami/spark/data/*.csv", header=True, inferSchema=True)
    df = raw

    df = df.withColumn("sale_total_price", col("sale_total_price").cast("double")) \
        .withColumn("product_price", col("product_price").cast("double")) \
        .withColumn("product_rating", col("product_rating").cast("double")) \
        .withColumn("product_reviews", col("product_reviews").cast("int")) \
        .withColumn("sale_date", to_date(col("sale_date"), "M/d/yyyy")) \
        .na.fill(0).na.fill("Unknown")

    dim_p = df.select("product_name", "product_category", "product_brand").distinct().withColumn("p_id",
                                                                                                 monotonically_increasing_id())
    dim_s = df.select("store_name", "store_city", "store_country").distinct().withColumn("s_id",
                                                                                         monotonically_increasing_id())
    dim_c = df.select("customer_email", "customer_first_name", "customer_last_name",
                      "customer_country").distinct().withColumn("c_id", monotonically_increasing_id())
    dim_sup = df.select("supplier_name", "supplier_country").distinct().withColumn("sup_id",
                                                                                   monotonically_increasing_id())
    dim_sel = df.select("seller_email", "seller_first_name", "seller_last_name",
                        "seller_country").distinct().withColumn("sel_id", monotonically_increasing_id())
    dim_t = df.select("sale_date").distinct().withColumn("t_id", monotonically_increasing_id())

    # Запись измерений в Postgres
    dims = [(dim_p, "dim_products"), (dim_s, "dim_stores"), (dim_c, "dim_customers"), (dim_sup, "dim_suppliers"),
            (dim_sel, "dim_sellers"), (dim_t, "dim_time")]
    for d, name in dims: d.write.jdbc(pg_url, name, mode="overwrite", properties=pg_props)

    fact = df.join(dim_p, ["product_name", "product_category", "product_brand"]) \
        .join(dim_s, ["store_name", "store_city", "store_country"]) \
        .join(dim_c, ["customer_email", "customer_first_name", "customer_last_name", "customer_country"]) \
        .join(dim_sup, ["supplier_name", "supplier_country"]) \
        .join(dim_sel, ["seller_email", "seller_first_name", "seller_last_name", "seller_country"]) \
        .join(dim_t, ["sale_date"]) \
        .select("p_id", "s_id", "c_id", "sup_id", "sel_id", "t_id",
                "product_name", "product_category", "store_name", "store_city", "store_country",
                "customer_first_name", "customer_last_name", "customer_country",
                "supplier_name", "supplier_country", "sale_date",
                "sale_total_price", "product_price", "product_rating", "product_reviews")

    fact.write.jdbc(pg_url, "fact_sales", mode="overwrite", properties=pg_props)


    # 1. Продукты: выручка, популярность, рейтинг, отзывы
    r1 = fact.groupBy("product_name", "product_category").agg(sum("sale_total_price"), count("*"),
                                                              avg("product_rating"), sum("product_reviews"))
    send_to_ch("rep1_products", r1)

    # 2. Клиенты: топ трат, страны, средний чек
    r2 = fact.withColumn("name", concat(col("customer_first_name"), lit(" "), col("customer_last_name"))) \
        .groupBy("name", "customer_country").agg(sum("sale_total_price"), avg("sale_total_price"))
    send_to_ch("rep2_customers", r2)

    # 3. Время: тренды, средний размер заказа
    r3 = fact.withColumn("y", year("sale_date")).withColumn("m", month("sale_date")).groupBy("y", "m").agg(
        sum("sale_total_price"), avg("sale_total_price"))
    send_to_ch("rep3_time", r3)

    # 4. Магазины: топ-5 выручки, локации, средний чек
    r4 = fact.groupBy("store_name", "store_city", "store_country").agg(sum("sale_total_price"), avg("sale_total_price"))
    send_to_ch("rep4_stores", r4)

    # 5. Поставщики: топ-5 выручки, средняя цена, страны
    r5 = fact.groupBy("supplier_name", "supplier_country").agg(sum("sale_total_price"), avg("product_price"))
    send_to_ch("rep5_suppliers", r5)

    # 6. Качество: рейтинги, корреляция (объем продаж), отзывы
    r6 = fact.groupBy("product_name").agg(avg("product_rating"), count("*"), sum("product_reviews"))
    send_to_ch("rep6_quality", r6)

if __name__ == "__main__":
    run_etl()