from pymongo import MongoClient, errors
from bson.json_util import dumps
import os
import json

MONGOPASS = os.getenv('MONGOPASS')
uri = "mongodb+srv://cluster0.pnxzwgz.mongodb.net/"
client = MongoClient(uri, username='nmagee', password=MONGOPASS, connectTimeoutMS=200, retryWrites=True)

db = client['ap6acf']
collection = db['mycoll']

directory_path = 'data/'

def import_data(directory):
    success_count = 0
    fail_count = 0
    corrupted_files = 0

    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.json'):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                        if isinstance(data, list):
                            collection.insert_many(data)
                        else:
                            collection.insert_one(data)
                        success_count += 1
                except json.JSONDecodeError:
                    print(f"Corrupted file detected: {file}")
                    corrupted_files += 1
                except Exception as e:
                    print(f"Failed to import {file}: {str(e)}")
                    fail_count += 1

    return success_count, fail_count, corrupted_files

results = import_data(directory_path)
print(f"Successfully imported {results[0]} files.")
print(f"Failed to import {results[1]} files.")
print(f"Corrupted files: {results[2]}")
