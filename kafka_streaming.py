
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType, StructField, LongType, ArrayType, MapType
from user_agents import parse
from util.config import Config
from pyspark.sql.window import Window
from util.logger import Log4j
import pycountry
import tldextract
from psycopg2 import pool
import psycopg2

def parse_browser(ua):
    user_agent = parse(ua)
    return user_agent.browser.family


def parse_os(ua):
    user_agent = parse(ua)
    return user_agent.os.family


def extract_country_from_domain(url):
    TLD_TO_COUNTRY = {country.alpha_2.lower(): country.name for country in pycountry.countries}
    extracted = tldextract.extract(url)
    tld = extracted.suffix
    country_name = TLD_TO_COUNTRY.get(tld, "Unknown")
    return country_name


def process_data(dataframe):
    log_schema = StructType([
        StructField("id", StringType(), True),
        StructField("api_version", StringType(), True),
        StructField("collection", StringType(), True),
        StructField("current_url", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("ip", StringType(), True),
        StructField("local_time", StringType(), True),
        StructField("option", ArrayType(StructType([
            StructField("option_id", StringType(), True),
            StructField("option_label", StringType(), True)
        ])), True),
        StructField("product_id", StringType(), True),
        StructField("referrer_url", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("time_stamp", LongType(), True),
        StructField("user_agent", StringType(), True)
    ])
    dataframe = dataframe.withColumn("value", expr("cast(value as string)"))
    dataframe = dataframe.withColumn("data", from_json(col("value"), log_schema)).selectExpr("data.*")
    option_df = dataframe.withColumn("option", explode(col("option")))
    # todo :
    option_df = (option_df.withColumn("option_id", col("option.option_id"))
                  .withColumn("option_label", col("option.option_label"))
                  .drop("option")
                  )
    option_df = option_df.withColumn("local_time", to_timestamp("local_time", "yyyy-MM-dd HH:mm:ss"))

    dataframe = (dataframe.withColumn("browser", parse_browser_udf("user_agent"))
                  .withColumn("os", parse_os_udf("user_agent"))
                  .drop("user_agent")
                  )
    dataframe = dataframe.withColumn("country", parse_country_udf("current_url"))
    dataframe = dataframe.withColumn("local_time", to_timestamp("local_time", "yyyy-MM-dd HH:mm:ss"))
    dataframe = dataframe.withColumn("year", year(col("local_time"))) \
        .withColumn("month", month(col("local_time"))) \
        .withColumn("day", dayofmonth(col("local_time"))) \
        .withColumn("hour", hour(col("local_time"))) \
        .withColumn("minute", minute(col("local_time"))) \
        .withColumn("second", second(col("local_time")))
    df_product = dataframe.filter(col("product_id").isNotNull()).select("product_id").distinct()
    # customer
    df_customer = dataframe.select("device_id","email").distinct()
    # store
    df_store = dataframe.select("store_id").distinct()
    # date
    df_date = dataframe.select("local_time", "day", "month", "year", "hour","minute","second").distinct()
    #log
    df_log = dataframe.filter(
        col("id").isNotNull() &
        col("device_id").isNotNull() &
        col("store_id").isNotNull() &
        col("product_id").isNotNull() &
        col("local_time").isNotNull()
    ).select("id", "collection", "referrer_url", "current_url", "device_id", "country", "store_id", "product_id",
             "local_time", "option", "browser", "os", "ip")
    df_log = df_log.withColumn("option", col("option").cast("string"))

    option_df = option_df.select(
        col("id").alias("log_id"),
        col("option_id"),
        col("option_label"),
        col("local_time")
    )
    return df_product,df_store,df_customer,df_date,df_log,option_df

# 🔹 Tạo connection pool (Min 1 - Max 5 connections)
conf = Config()
postgres_conf = conf.postgres_conf
connection_pool = pool.SimpleConnectionPool(1, 5, **postgres_conf)

def write_partition(iterator, insert_query):
    postgres_conf = conf.postgres_conf
    conn = psycopg2.connect(**postgres_conf)
    cursor = conn.cursor()
    for row in iterator:
        try:
            print(f"📝 Inserting: {row}")
            cursor.execute(insert_query, row)
        except Exception as e:
            print(f"❌ Error inserting data: {e}")

    conn.commit()
    cursor.close()
    conn.close()

# 🔹 Hàm output để insert nhiều bảng
def output(kafka_df, batch_id):
    df_product, df_store, df_customer, df_date, df_log, option_df = process_data(kafka_df)

    df_product.foreachPartition(lambda iterator: write_partition(iterator,
                                                                  "INSERT INTO dim_product (product_id) VALUES (%s) ON CONFLICT (product_id) DO NOTHING"
                                                                  ))

    df_customer.foreachPartition(lambda iterator: write_partition(iterator,
                                                                  "INSERT INTO dim_customer (device_id, email) VALUES (%s, %s) ON CONFLICT (device_id) DO NOTHING"
                                                                  ))

    df_store.foreachPartition(lambda iterator: write_partition(iterator,
                                                               "INSERT INTO dim_store (store_id) VALUES (%s) ON CONFLICT (store_id) DO NOTHING"
                                                               ))

    df_date.foreachPartition(lambda iterator: write_partition(iterator,
                                                              "INSERT INTO dim_date (local_time, day, month, year, hour, minute, second) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (local_time) DO NOTHING"
                                                              ))

    df_log.foreachPartition(lambda iterator: write_partition(iterator,
                                                             "INSERT INTO fact_logs (id, collection, referrer_url, current_url, device_id, country, store_id, product_id, local_time, option, browser, os, ip) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                                                           ))

    option_df.foreachPartition(lambda iterator: write_partition(iterator,
                                                                "INSERT INTO dim_option (log_id, option_id, option_label, local_time) VALUES (%s, %s, %s, %s)"
                                                                ))


if __name__ == '__main__':
    conf = Config()
    spark_conf = conf.spark_conf
    kaka_conf = conf.kafka_conf
    parse_browser_udf = udf(parse_browser, returnType=StringType())
    parse_os_udf = udf(parse_os, returnType=StringType())
    parse_country_udf = udf(extract_country_from_domain,returnType=StringType())
    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    kafka_df = spark.readStream\
        .format("kafka") \
        .options(**kaka_conf) \
        .load()
    (kafka_df
     .writeStream
     .foreachBatch(output)
     .trigger(processingTime='30 seconds')
     .start()
     .awaitTermination())








