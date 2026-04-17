import urllib.request
import csv
import io
from base64 import b64encode
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("StarSchema_NoLimit_Perfect").getOrCreate()

pg_url = "jdbc:postgresql://postgres:5432/main_db"
pg_props = {"user": "user", "password": "password", "driver": "org.postgresql.Driver"}
CH_URL = "http://clickhouse:8123/"
CH_AUTH = f"Basic {b64encode(b'user:password').decode('ascii')}"


def send_partition_to_ch(partition_data, table_name):
    rows = list(partition_data)
    if not rows: return
    output = io.StringIO()
    writer = csv.writer(output, quoting=csv.QUOTE_NONNUMERIC)
    for r in rows: writer.writerow(list(r))
    csv_body = output.getvalue()
    url = f"{CH_URL}?query=INSERT+INTO+reports.{table_name}+FORMAT+CSV"
    req = urllib.request.Request(url, data=csv_body.encode("utf-8"), headers={"Authorization": CH_AUTH})
    urllib.request.urlopen(req)


def run_etl():
    print("\n>>> ШАГ 1: ЗАГРУЗКА И ДЕДУПЛИКАЦИЯ <<<")
    # Читаем все CSV
    raw_df = spark.read.csv("/opt/bitnami/spark/data/*.csv", header=True, inferSchema=True)

    df = raw_df.dropDuplicates()

    print(f"Строк после удаления дублей: {df.count()}")  # Здесь должно быть 10 000

    df = df.withColumn("sale_total_price", col("sale_total_price").cast("double")) \
        .withColumn("product_rating", col("product_rating").cast("double")) \
        .withColumn("product_reviews", col("product_reviews").cast("int")) \
        .withColumn("sale_date", to_date(col("sale_date"), "M/d/yyyy")).na.fill(0).na.fill("Unknown")

    print("\n>>> ШАГ 2: СОЗДАНИЕ 6 ИЗМЕРЕНИЙ <<<")

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
    dim_t = df.select("sale_date").distinct().withColumn("t_id", monotonically_increasing_id()).withColumn("y", year(
        "sale_date")).withColumn("m", month("sale_date"))

    # Сохраняем в Postgres
    dims = [(dim_p, "dim_products"), (dim_s, "dim_stores"), (dim_c, "dim_customers"), (dim_sup, "dim_suppliers"),
            (dim_sel, "dim_sellers"), (dim_t, "dim_time")]
    for d, name in dims: d.write.jdbc(pg_url, name, mode="overwrite", properties=pg_props)

    print("\n>>> ШАГ 3: СБОРКА ТАБЛИЦЫ ФАКТОВ <<<")
    fact = df.join(dim_p, ["product_name", "product_category", "product_brand"]) \
        .join(dim_s, ["store_name", "store_city", "store_country"]) \
        .join(dim_c, ["customer_email", "customer_first_name", "customer_last_name", "customer_country"]) \
        .join(dim_sup, ["supplier_name", "supplier_country"]) \
        .join(dim_sel, ["seller_email", "seller_first_name", "seller_last_name", "seller_country"]) \
        .join(dim_t, ["sale_date"]) \
        .select("p_id", "s_id", "c_id", "sup_id", "sel_id", "t_id",
                "sale_total_price", "product_rating", "product_reviews")


    fact.write.jdbc(pg_url, "fact_sales", mode="overwrite", properties=pg_props)

    print("\n>>> ШАГ 4: 6 ВИТРИН <<<")

    r1 = fact.join(dim_p, "p_id").groupBy("product_name", "product_category").agg(sum("sale_total_price"), count("*"),
                                                                                  avg("product_rating"),
                                                                                  sum("product_reviews"))
    r1.foreachPartition(lambda p: send_partition_to_ch(p, "rep1_products"))

    r2 = fact.join(dim_c, "c_id").withColumn("n",
                                             concat(col("customer_first_name"), lit(" "), col("customer_last_name"))) \
        .groupBy("n", "customer_country").agg(sum("sale_total_price"), avg("sale_total_price"))
    r2.foreachPartition(lambda p: send_partition_to_ch(p, "rep2_customers"))

    r3 = fact.join(dim_t, "t_id").groupBy("y", "m").agg(sum("sale_total_price"), avg("sale_total_price"))
    r3.foreachPartition(lambda p: send_partition_to_ch(p, "rep3_time"))

    r4 = fact.join(dim_s, "s_id").groupBy("store_name", "store_city", "store_country").agg(sum("sale_total_price"),
                                                                                           avg("sale_total_price"))
    r4.foreachPartition(lambda p: send_partition_to_ch(p, "rep4_stores"))

    r5 = fact.join(dim_sup, "sup_id").groupBy("supplier_name", "supplier_country").agg(sum("sale_total_price"),
                                                                                       avg("sale_total_price"))
    r5.foreachPartition(lambda p: send_partition_to_ch(p, "rep5_suppliers"))

    r6 = fact.join(dim_p, "p_id").groupBy("product_name").agg(avg("product_rating"), count("*"), sum("product_reviews"))
    r6.foreachPartition(lambda p: send_partition_to_ch(p, "rep6_quality"))

    print("\n--- ВСЁ ГОТОВО! ---")


if __name__ == "__main__":
    run_etl()