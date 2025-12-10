from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

def load_data(file_path):
    spark = SparkSession.builder.getOrCreate()
    data = spark.read.csv(file_path, header=True, inferSchema=True)
    return data

## Melakukan load data
df= load_data('/opt/airflow/data/extract_dataset_global_coffee_health.csv')

## Mengisi baris yang kosong pada kolom `Health_Issues`
df = df.fillna({'Health_Issues': 'None'})

## Mengubah tipe data `Smoking` dan `Alcohol_Consumption` dari boolean menjadi string
df = df.withColumn("Smoking", col("Smoking").cast("string")) \
       .withColumn("Alcohol_Consumption", col("Alcohol_Consumption").cast("string"))

## Mengubah value data di kolom `Smoking` dan `Alcohol_Consumption` dari 0 & 1 menjadi No & Yes
df = df.withColumn("Smoking", when(col("Smoking") == "1", "Yes").otherwise("No")) \
       .withColumn("Alcohol_Consumption", when(col("Alcohol_Consumption") == "1", "Yes").otherwise("No"))

df.show(10)

## Save csv transform
df.coalesce(1).write.csv('/opt/airflow/data/transform_dataset_global_coffee_health.csv', header=True, mode='overwrite')