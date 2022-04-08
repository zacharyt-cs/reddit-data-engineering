import pendulum
import requests
import json
import time

url = "https://api.pushshift.io/reddit/search"

def fetchObjects(mode, **kwargs):
    
    # Default parameters
    # Change as necessary/desired
    params = {
        "sorted_type": "created_utc",
        "sort": "asc",
        "size": "1000"
        }

    # Add additional parameters based on function arguments
    for key, value in kwargs.items():
        params[key] = value
    
    loop = True
    while loop:
        # Perform API request
        r = requests.get(f'{url}/{mode}/', params=params, timeout=90)
        # print(r.url)
        if r.status_code != 200:
            print(r.status_code)
            print("Retrying...")
        else:
            # successful (200), loop = False and process data
            loop = False
    else:
        response = json.loads(r.text)
        data = response['data']
        sorted_data_by_id = sorted(data, key=lambda x: int(x['id'],36))
        return sorted_data_by_id

def extract_reddit_data(subreddit, mode, start, end, filepath):

    # arg datetime format: pendulum.DateTime
    start = pendulum.parse(start)
    end = pendulum.parse(end)

    # convert DateTime to timestamp to pass into API
    start_ts = start.int_timestamp
    end_ts = end.int_timestamp
    
    max_id = 0
    
    # Open file for JSON output
    file = open(filepath, "a")

    while True: 
        nothing_processed = True
        objects = fetchObjects(mode, subreddit=subreddit, after=start_ts, before=end_ts)
        
        for object in objects:
            id = int(object['id'],36)
            if id > max_id:
                nothing_processed = False
                created_utc = object['created_utc']
                max_id = id
                if created_utc > start_ts:
                    start_ts = created_utc
                # Output JSON data to the opened file
                file.write(json.dumps(object,sort_keys=True,ensure_ascii=True) + "\n")

        # Exit if nothing happened
        if nothing_processed: break
        start_ts -= 1

        # Sleep a little before the next function call
        time.sleep(.5)
    
    file.close()