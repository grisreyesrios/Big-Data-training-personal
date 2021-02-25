from pyspark.sql import SparkSession

name = "spark_demo"
file = "gs://dataflow-excercise/test-dataset.avro"
table_name = "lively-armor-283518:apache_beam.avrotable4"

#Iniciar sesión de spar

spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName(name) \
  .getOrCreate()

def main():
    
    #Leer avro de gcs
	df = spark.read.format("com.databricks.spark.avro").load(file, header='true')

	#Mostrar esquema y primeras líneas
	df.show()

	#Escribir a BigQuery
	df.write \
	.format("bigquery") \
	.option("temporaryGcsBucket","dataproc-excercise") \
	.save(table_name)

if __name__ == "__main__":  
	main()
	print("\n\nFinished!\n\n")
