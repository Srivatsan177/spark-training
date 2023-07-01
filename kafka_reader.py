from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("kafka-spark-streaming").getOrCreate()

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "quickstart-events")
    .load()
)
cnt = df.selectExpr("CAST(KEY as STRING)", "CAST(VALUE as STRING)")

query = cnt.writeStream.format("console").start()

query.awaitTermination()
