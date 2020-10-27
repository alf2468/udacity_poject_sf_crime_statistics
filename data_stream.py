import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# TODO Create a schema for incoming resources
schema = StructType([
    StructField("crime_id", StringType()),
    StructField("original_crime_type_name", StringType()),
    StructField("report_date", TimestampType()),
    StructField("call_date", TimestampType()),
    StructField("offense_date", TimestampType()),
    StructField("call_time", StringType()),
    StructField("call_date_time", TimestampType()),
    StructField("disposition", StringType()),
    StructField("address", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("agency_id", StringType()),
    StructField("address_type", StringType()),
    StructField("common_location", StringType())
])


def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "org.san-francisco.police-department-calls") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 500) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka_config input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value as STRING)")

    service_table = kafka_df \
        .select(psf.from_json(psf.col('value'), schema).alias("DF")) \
        .select("DF.*")


    # TODO select original_crime_type_name and disposition
    distinct_table = service_table.select(["call_date_time", "original_crime_type_name", "disposition"])

    # count the number of original crime type
    agg_df = distinct_table \
        .withWatermark("call_date_time", "60 minutes") \
        .groupBy(
            psf.window(distinct_table.call_date_time, "60 minutes", "10 minutes"),
            distinct_table.original_crime_type_name,
            distinct_table.disposition) \
        .count()

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query = agg_df.writeStream.format('console').trigger(once=True).outputMode('Update').start()

    # TODO attach a ProgressReporter
    query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = "./radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath, multiLine=True)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # TODO join on disposition column
    join_query = agg_df. \
        join(radio_code_df, "disposition", "left_outer"). \
        writeStream.trigger(processingTime="10 seconds"). \
        outputMode("Update"). \
        format("console"). \
        start()

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.streaming.kafka.maxRatePerPartition", 50) \
        .config("spark.sql.shuffle.partitions", 50) \
        .config("spark.default.parallelism", 6) \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
