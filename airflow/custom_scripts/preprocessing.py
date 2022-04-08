import csv
import json
import logging
import pyarrow.csv as pv
import pyarrow.parquet as pq


def json_to_csv(json_filepath, csv_filepath, mode):

    with open(json_filepath) as json_file:
        json_list = list(json_file)
    # Create new file to write to
    newfile = open(csv_filepath, 'w', encoding='utf-8', newline='')
    csv_writer = csv.writer(newfile)
    
    if mode == 'submission':
        csv_writer.writerow(["id","title","author", "created_utc", "num_comments","total_awards_received"])
        for json_str in json_list:
            # convert string to json object
            result = json.loads(json_str)
            # write each column, row by row
            id = result['id']
            title = result['title']
            author = result['author']
            created_utc = result['created_utc']
            num_comments = result['num_comments']
            total_awards_received = result['total_awards_received']
            csv_writer.writerow([id, title, author, created_utc, num_comments, total_awards_received])
        newfile.close()
    
    elif mode == 'comment':
        csv_writer.writerow(["id", "author", "created_utc", "body", "total_awards_received"])
        for json_str in json_list:
            # convert string to json object
            result = json.loads(json_str)
            # write each column, row by row
            id = result['id']
            author = result['author']
            created_utc = result['created_utc']
            body = result['body']
            total_awards_received = result['total_awards_received']
            csv_writer.writerow([id, title, author, created_utc, body, total_awards_received])
        newfile.close()

def csv_to_parquet(csv_filepath, parquet_filepath):
    
    if not csv_filepath.endswith('.csv'):
        logging.error("Not a CSV file")
        return
    table = pv.read_csv(csv_filepath)
    pq.write_table(table, parquet_filepath)