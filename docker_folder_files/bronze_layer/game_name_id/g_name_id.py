from pandas.io import parquet
import requests
import pandas as pd
import numpy as np
import os 
import boto3
import time
from io import BytesIO

s3 = boto3.client("s3")
def handler(event,context):
        bucket = os.environ["vgsa"]
        prefix = "bronze/"
        ##Fetch the app ids of all games:
        request_app_id = requests.get("https://api.steampowered.com/ISteamApps/GetAppList/v2/")
        request_app_id_json = request_app_id.json()
        app_id_df = pd.json_normalize(request_app_id_json["applist"]["apps"])        
        parquet_buffer = BytesIO()
        app_id_df.to_parquet(parquet_buffer,index=False)
        key_app_id = f"{prefix}game_id/app_id.parquet"
        s3.put_object(Bucket=bucket,Key=key_app_id,Body=parquet_buffer.getvalue())
        return {
        "status": "success",
        "app_id_s3": f"s3://{bucket}/{key_app_id}"}