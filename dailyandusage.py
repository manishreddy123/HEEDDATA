# dailyandusage.py

import os
import pandas as pd
from pymongo import MongoClient
from dbconnection import MongoDBConnection
from model import SchemaValidator

class DailyAndUsage:
    def __init__(self):
        self.validator = SchemaValidator()

    def datamanipulation(self, folder_path):
        self.folder_path = folder_path
        collection_names_list = ["daily_usage", "usage_summary"]
        mongo_conn = MongoDBConnection()
        self.collections = mongo_conn.setup_database_and_collections(collection_names_list)
        collections_dict, self.db_name, self.db_uri = self.collections
        self.client = MongoClient(self.db_uri)
        self.db = self.client[self.db_name]
        self.daily_usage_collection = self.db['daily_usage']  
        self.usage_summary_collection = self.db['usage_summary'] 
        for filename in os.listdir(self.folder_path):
            if filename.endswith('.csv'):
                self.file_path = os.path.join(self.folder_path, filename)
                self.process_file()
                print("file in path ",{self.file_path},"completed")

    def validate_and_update(self, collection, query, update, schema_name):
        collection.update_one(query, update)
        updated_doc = collection.find_one(query)
        self.validator.validate_schema(schema_name, updated_doc)

    def validate_and_insert(self, collection, document, schema_name):
        self.validator.validate_schema(schema_name, document)
        collection.insert_one(document)

    def process_file(self):
        df = pd.read_csv(self.file_path, low_memory=False)
        for _, row in df.iterrows():
            resource_id = row['lineItem/ResourceId']
            if not pd.isna(resource_id):
                usage_start_date = row['lineItem/UsageStartDate'][:10] #  assuming lineItem/UsageStartDate format is YYYY-MM-DDThh:mm:ss....
                daily_usage_doc = self.daily_usage_collection.find_one({
                    "lineItem/UsageDate": usage_start_date,
                    "lineItem/ResourceId": resource_id
                })
                if daily_usage_doc:
                    self.validate_and_update(
                        self.daily_usage_collection,
                        {"_id": daily_usage_doc["_id"]},
                        {
                            "$inc": {
                                "lineItem/UsageAmount": row['lineItem/UsageAmount'],
                                "lineItem/UnblendedRate": row['lineItem/UnblendedRate'],
                                "lineItem/UnblendedCost": row['lineItem/UnblendedCost'],
                                "lineItem/BlendedRate": row['lineItem/BlendedRate'],
                                "lineItem/BlendedCost": row['lineItem/BlendedCost']
                            }
                        },
                        "daily_usage"
                    )
                else:
                    new_document = row.to_dict()
                    new_document['lineItem/UsageDate'] = usage_start_date
                    self.validate_and_insert(
                        self.daily_usage_collection,
                        new_document,
                        "daily_usage"
                    )
                usage_summary_doc = self.usage_summary_collection.find_one({
                    "lineItem/ResourceId": resource_id
                })
                if usage_summary_doc:
                    self.validate_and_update(
                        self.usage_summary_collection,
                        {"_id": usage_summary_doc["_id"]},
                        {
                            "$inc": {
                                "lineItem/UsageAmount": row['lineItem/UsageAmount'],
                                "lineItem/UnblendedRate": row['lineItem/UnblendedRate'],
                                "lineItem/UnblendedCost": row['lineItem/UnblendedCost'],
                                "lineItem/BlendedRate": row['lineItem/BlendedRate'],
                                "lineItem/BlendedCost": row['lineItem/BlendedCost']
                            },
                            "$set": {
                                "lineItem/UsageEndDate": row['lineItem/UsageEndDate']
                            }
                        },
                        "usage_summary"
                    )
                else:
                    self.validate_and_insert(
                        self.usage_summary_collection,
                        row.to_dict(),
                        "usage_summary"
                    )

    '''def process_folder(self):
        folder_path = self.folder_path
        for filename in os.listdir(folder_path):
            if filename.endswith('.csv'):
                file_path = os.path.join(folder_path, filename)
                self.process_file(file_path)'''



