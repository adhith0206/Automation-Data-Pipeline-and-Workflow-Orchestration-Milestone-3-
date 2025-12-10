import os
import glob
import pandas as pd
from pymongo import MongoClient

## Folder path
folder_path = '/opt/airflow/data/transform_dataset_global_coffee_health.csv'

## Koneksi ke MongoDB
client = MongoClient("mongodb+srv://adhit0206:%40Kakeru02061999@adhit-coda10.k7cszt5.mongodb.net/")
db = client["P2M3_Adhit_Hikmatullah"]
collection = db["global_coffee_health"]

## Melakukan read csv
csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
for file_path in csv_files:
    df = pd.read_csv(file_path)
    data = df.to_dict("records")
    if data:
        collection.insert_many(data)