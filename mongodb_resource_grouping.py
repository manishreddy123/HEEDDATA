import os
import pandas as pd
from pymongo import MongoClient

class CSVProcessor:
    def __init__(self, input_folder, output_folder, max_size_mb):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.max_size_mb = max_size_mb

    def split_csv_by_size(self, input_file_path):
        max_size_bytes = self.max_size_mb * 1024 * 1024  # Convert MB to bytes
        part_num = 0
        with open(input_file_path, 'r', encoding='utf-8') as input_file:
            header = input_file.readline()
            output_file = open(os.path.join(self.output_folder, f"{os.path.basename(input_file_path)}_part{part_num}.csv"), 'w', encoding='utf-8')
            output_file.write(header)
            current_size = len(header.encode('utf-8'))
            for line in input_file:
                line_size = len(line.encode('utf-8'))
                if current_size + line_size > max_size_bytes:
                    output_file.close()
                    part_num += 1
                    output_file = open(os.path.join(self.output_folder, f"{os.path.basename(input_file_path)}_part{part_num}.csv"), 'w', encoding='utf-8')
                    output_file.write(header)
                    current_size = len(header.encode('utf-8'))
                output_file.write(line)
                current_size += line_size
            output_file.close()

    def process_csv_files(self):
        os.makedirs(self.output_folder, exist_ok=True)
        for filename in os.listdir(self.input_folder):
            if filename.endswith('.csv'):
                file_path = os.path.join(self.input_folder, filename)
                file_size = os.path.getsize(file_path)
                if file_size > self.max_size_mb * 1024 * 1024:
                    self.split_csv_by_size(file_path)
                else:
                    output_file_path = os.path.join(self.output_folder, filename)
                    with open(file_path, 'r', encoding='utf-8') as input_file, \
                         open(output_file_path, 'w', encoding='utf-8') as output_file:
                        output_file.write(input_file.read())

class DataManipulation:
    def __init__(self, db_uri: str, db_name: str, daily_usage_collection: str, usage_summary_collection: str):
        self.client = MongoClient(db_uri)
        self.db = self.client[db_name]
        self.daily_usage_collection = self.db[daily_usage_collection]  
        self.usage_summary_collection = self.db[usage_summary_collection]  

    def process_file(self, file_path: str):
        df = pd.read_csv(file_path, low_memory=False)
        for _, row in df.iterrows():
            resource_id = row['lineItem/ResourceId']
            if not pd.isna(resource_id):
                usage_start_date = row['lineItem/UsageStartDate'][:10] #  assuming lineItem/UsageStartDate format is YYYY-MM-DDThh:mm:ss....
                daily_usage_doc = self.daily_usage_collection.find_one({
                    "lineItem/UsageDate": usage_start_date,
                    "lineItem/ResourceId": resource_id
                })
                if daily_usage_doc:
                    self.daily_usage_collection.update_one(
                        {"_id": daily_usage_doc["_id"]},
                        {
                            "$inc": {
                                "lineItem/UsageAmount": row['lineItem/UsageAmount'],
                                "lineItem/UnblendedRate": row['lineItem/UnblendedRate'],
                                "lineItem/UnblendedCost": row['lineItem/UnblendedCost'],
                                "lineItem/BlendedRate": row['lineItem/BlendedRate'],
                                "lineItem/BlendedCost": row['lineItem/BlendedCost']
                            }
                        }
                    )
                else:
                    new_document = row.to_dict()
                    new_document['lineItem/UsageDate'] = usage_start_date
                    self.daily_usage_collection.insert_one(new_document)
                usage_summary_doc = self.usage_summary_collection.find_one({
                    "lineItem/ResourceId": resource_id
                })
                if usage_summary_doc:
                    self.usage_summary_collection.update_one(
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
                        }
                    )
                else:
                    self.usage_summary_collection.insert_one(row.to_dict())

    def process_folder(self, folder_path: str):
        for filename in os.listdir(folder_path):
            if filename.endswith('.csv'):
                file_path = os.path.join(folder_path, filename)
                self.process_file(file_path)

class CSVDataHandler:
    def __init__(self, input_folder: str, output_folder: str, max_size_mb: int, db_uri: str, db_name: str, daily_usage_collection: str, usage_summary_collection: str):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.max_size_mb = max_size_mb
        self.db_uri = db_uri
        self.db_name = db_name
        self.daily_usage_collection = daily_usage_collection
        self.usage_summary_collection = usage_summary_collection

    def run(self):
        csv_processor = CSVProcessor(self.input_folder, self.output_folder, self.max_size_mb)
        csv_processor.process_csv_files()
        data_manipulator = DataManipulation(self.db_uri, self.db_name, self.daily_usage_collection, self.usage_summary_collection)
        data_manipulator.process_folder(self.output_folder)

if __name__ == "__main__":
    input_folder = 'C:/Users/manis/Documents/HEEDDATA/input_fol'  # input folder path where all the csv files are present
    output_folder = 'C:/Users/manis/Documents/HEEDDATA/output_fol'  # output folder path where all the csv files will be saved after splitting them into smaller size
    max_size_mb = 300  # maximum size in mb each csv file can be
    db_uri = 'mongodb://localhost:27017/'  #  database address
    db_name = 'heeddata'  # database name
    daily_usage_collection = 'daily_usage'  # daily usage summary collection
    usage_summary_collection = 'usage_summary'  # total usage summary collection
    
    handler = CSVDataHandler(input_folder, output_folder, max_size_mb, db_uri, db_name, daily_usage_collection, usage_summary_collection)
    handler.run()