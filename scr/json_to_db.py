import os
import json
import shutil
import time

def read_json_file(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data

def process_data(data):
    # Process the data here...
    processed_data = data
    return processed_data

def move_file_to_output_folder(file_path, output_folder):
    timestamp = int(time.time())
    file_name = os.path.basename(file_path)
    output_path = os.path.join(output_folder, f"{timestamp}_{file_name}")
    os.makedirs(output_folder, exist_ok=True)
    shutil.move(file_path, output_path)

def process_files_in_folder(input_folder, output_folder):
    os.makedirs(output_folder, exist_ok=True)
    for file_name in os.listdir(input_folder):
        if file_name.endswith(".json"):
            input_file_path = os.path.join(input_folder, file_name)
            data = read_json_file(input_file_path)
            processed_data = process_data(data)
            move_file_to_output_folder(input_file_path, output_folder)

if __name__ == "__main__":
    input_folder = "input"
    output_folder = "out"
    process_files_in_folder(input_folder, output_folder)
