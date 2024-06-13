# csvprocessor.py

import os
from constants import INPUT_FOLDER, MAX_SIZE_MB
from dailyandusage import DailyAndUsage

import os
from constants import INPUT_FOLDER, MAX_SIZE_MB
from dailyandusage import DailyAndUsage

class CSVProcessor:
    def __init__(self):
        self.input_folder = INPUT_FOLDER
        self.output_folder = os.path.join(os.getcwd(), 'output_fol')
        os.makedirs(self.output_folder, exist_ok=True)
        self.max_size_mb = MAX_SIZE_MB
        self.process_csv_files()

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
       
        data_manipulation = DailyAndUsage()
        data_manipulation.datamanipulation(self.output_folder)





#  code to be used in main function
# CSVProcessor()
#x = mongo.process_csv_files()
#  x is the output folder path 