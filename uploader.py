import pandas as pd
from pymongo import MongoClient

def upload_csv_to_mongodb(csv_file, db_name, collection_name, mongo_uri="mongodb://localhost:27017/"):
    # Membaca file CSV
    try:
        df = pd.read_csv(csv_file)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    # Koneksi ke MongoDB
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return

    # Mengonversi DataFrame ke list of dictionaries
    data = df.to_dict(orient='records')

    # Upload data ke MongoDB
    try:
        collection.insert_many(data)
        print(f"Data berhasil di-upload ke MongoDB dalam collection '{collection_name}' di database '{db_name}'")
    except Exception as e:
        print(f"Error uploading data to MongoDB: {e}")
    finally:
        client.close()

# Contoh penggunaan
csv_file_path = '/Users/macbookpro/Documents/uploaderCSV/tags.csv'
database_name = 'movieLens'
collection_name = 'tags'

upload_csv_to_mongodb(csv_file_path, database_name, collection_name)
