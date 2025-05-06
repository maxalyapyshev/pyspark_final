from pyspark.sql import SparkSession
import pyspark.sql.functions as F


spark = (
    SparkSession.builder.appName("web_server_logs")
        .master("local[*]")
        .getOrCreate()
)

df_logs = spark.read.csv(path="web_server_logs.csv",
                         header=True,
                         sep=",",
                         inferSchema=True)

df_top_ip = (
    df_logs.groupBy("ip")
        .agg(F.count(F.lit(1)).alias("request_count"))
        .orderBy(F.col("request_count").desc())
        .limit(10)
)
print("Top 10 active IP addresses:")
df_top_ip.show()

df_methods = df_logs.groupBy("method").agg(F.count(F.lit(1)).alias("method_count"))
print("Request count by HTTP-method:")
df_methods.show()

err_cnt = df_logs.filter(F.col("response_code") == 404).count()
print(f"Number of 404 response codes: {err_cnt}")

df_response_by_date = (
    df_logs.withColumn("date", F.to_date("timestamp"))
        .groupBy("date")
        .agg(F.sum(F.col("response_size")).alias("total_response_size"))
        .orderBy("date")
)
print("Total response size by date:")
df_response_by_date.show()
