import requests
from datetime import date, timedelta, datetime
import pandas as pd
import os
from google.cloud import storage


def upload_to_gcs(bucket_name, destination_blob_name, source_file_name):
    """Uploads a file to Google Cloud Storage."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

def fetch_news(api_key, query, from_date, to_date, bucket_name, destination):
    base_url = "https://newsapi.org/v2/everything?q={}&from={}&to={}&sortBy=popularity&apiKey={}&language=en"
    
    url_extractor = base_url.format(query, from_date, to_date, api_key)
    response = requests.get(url_extractor)
    if response.status_code == 200:
        res = response.json()
        df = pd.DataFrame(columns=['newsTitle', 'timestamp', 'url_source', 'content', 'source', 'author', 'urlToImage'])

        for data in res["articles"]:
            partial_content = ""
            if data["content"] not in [None, "", " "]:
                partial_content = data["content"]
                if len(partial_content) > 200:
                    partial_content = partial_content[:199]
                if '.' in partial_content:
                    partial_content = partial_content[:partial_content.rindex('.')+1]
            new_row = pd.DataFrame({
                "newsTitle": [data["title"]],
                "timestamp": [data["publishedAt"]],
                "url_source": [data["url"]],
                "content": [partial_content],
                "source": [data["source"]["name"]],
                "author": [data["author"]],
                "urlToImage": [data["urlToImage"]]
            })
            df = pd.concat([df, new_row], ignore_index=True)
        
        current_time =  datetime.today().strftime('%Y%m%d%H%M%S')
        filename = f'run-{current_time}.parquet'
        
        print(df)
        
        # Check and print the current working directory
        print("Current Working Directory:", os.getcwd())
        
        df.to_parquet(filename)
        
         # Upload to GCS
         
        destination_blob_name = f'{destination}/{filename}'
        upload_to_gcs(bucket_name, destination_blob_name, filename)
        
        # Remove local file after upload
        os.remove(filename)
    else:
        print("Failed to fetch news articles")