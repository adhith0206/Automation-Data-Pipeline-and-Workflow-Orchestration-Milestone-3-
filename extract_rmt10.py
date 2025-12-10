from pyspark.sql import SparkSession
def load_data(file_path):
    spark = SparkSession.builder.getOrCreate()
    data = spark.read.csv(file_path, header=True, inferSchema=True)
    return data

## Melakukan load data
df= load_data('/opt/airflow/data/P2M3_Adhit_Hikmatullah_Data_Raw.csv')
df.show(10)

## Save csv extract
df.coalesce(1).write.csv('/opt/airflow/data/extract_dataset_global_coffee_health.csv', header=True, mode='overwrite')