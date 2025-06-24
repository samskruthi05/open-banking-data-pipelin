import json
import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

"""
Module for extracting data from JSON files.
"""

def read_json_files(file_paths):
    all_records = []
    for path in file_paths:
        try:
            with open(path) as f:
                data = json.load(f)
        except FileNotFoundError:
            logging.warning(f'file not found : {path}')
            continue
            #print(data)
        #checking if the json is dict 
        if isinstance(data, dict) and "transactions" in data:
                all_records.extend(data["transactions"])
        #checking if the json is a list
        if isinstance(data, list):
                all_records.extend(data["transactions"])
    #print(all_records[0])
    return all_records

if __name__ == "__main__":

    file_paths = [
        'data/tech_assessment_transactions_01.json',
        'data/tech_assessment_transactions_02.json'
    ]
    records = read_json_files(file_paths)
    print(f'total records {len(records)}')
    print(f'this is the first row {records[0]}')



        
        



   
      
        








