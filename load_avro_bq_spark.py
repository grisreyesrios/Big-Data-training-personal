from pyspark.sql import SparkSession
import json

PROJECT_ID = 'lively-armor-283518'
DATASET_NAME = 'apache_beam'
# Use the Cloud Storage bucket for temporary BigQuery export data used by the connector.
BUCKET = "gs://dataproc-excercise"

spark = SparkSession.builder.\
            appName("GCSFilesRead").\
            config("spark.jars.packages", "org.apache.spark:spark-avro_2.11:2.4.0").\
            getOrCreate()
            # config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.18.1").\ 
            # Couldn't import this jar from maven repository, downloaded the corresponding jar instead


# Load data from GCS and schema

# Read the schema

# A JSON dataset is pointed to by path.
# The path can be either a single text file or a directory storing text files
path = "gs://dataflow-excercise/test-schema.json"
SchemaDF = spark.read.json(path)

# The inferred schema can be visualized using the printSchema() method
#SchemaDF.printSchema()


#jsonFormatSchema = open("gs://dataflow-excercise/test-schema.json", "r").read()

df = spark.read.format("avro").load("gs://dataflow-excercise/test-dataset.avro")
df.select(SchemaDF)


# Saving the data to BigQuery
df.write \
  .format("bigquery") \
  .option("temporaryGcsBucket", BUCKET) \
  .save("lively-armor-283518:apache_beam.avrotable2")

print("\n\nFinished!\n\n")
