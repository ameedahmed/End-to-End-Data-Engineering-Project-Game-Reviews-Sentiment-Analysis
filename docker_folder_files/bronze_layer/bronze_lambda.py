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

        #Find the top selling games on steam

        params = {"filter": "topsellers",
                "hidef2p": 1,
                "page": 1,
                "json":1}

        #Fetch the top selling games
        url = requests.get("https://store.steampowered.com/search/results/", params=params)

        print(url)
        search_results = url.json()
        
        search_results_df = pd.json_normalize(search_results["items"])
        search_results_df = search_results_df.drop(search_results_df[search_results_df["name"]=="Steam Deck"].index)
        
        
        ##Save the top selling games in parquet format:
        parquet_buffer = BytesIO()
        
        search_results_df.to_parquet(parquet_buffer,index=False)
                
        timestamp = int(time.time())
        
        key_topsellers = f"{prefix}top_selling_games/topsellers.parquet"
        
        s3.put_object(Bucket=bucket,Key=key_topsellers, Body = parquet_buffer.getvalue())

        ##Now fetch the app ids of all games:
        request_app_id = requests.get("https://api.steampowered.com/ISteamApps/GetAppList/v2/")
        request_app_id_json = request_app_id.json()
        app_id_df = pd.json_normalize(request_app_id_json["applist"]["apps"])
        merged_df = pd.merge(search_results_df,app_id_df,on="name",how="inner")
        merged_df = merged_df.drop_duplicates(subset="name")        
        
        #Now fetch the reviews for ids in top sellers
        key_rsm_processed_files = []
        key_rev_processed_files = []
        for id in merged_df["appid"]:
                params_reviews = {"json":1,"cursor":"*","num_per_page":100,"filter":"recent"}
                df_reviews = pd.DataFrame()
                try:
                        reviews_count = 0
                        while True:
                                user_review_url = f'https://store.steampowered.com/appreviews/{id}'
                                request_review = requests.get(user_review_url,params = params_reviews)
                                user_review_json = request_review.json()
                                
                                if reviews_count==0:
                                        review_query_summary = user_review_json["query_summary"]
                                        #Now save the query summary in form of parquet for the overlapping ids
                                        review_summary_df = pd.json_normalize(review_query_summary)
                                        parquet_buffer = BytesIO()
                                        review_summary_df.to_parquet(parquet_buffer,index=False)
                                        key_rsm = f"{prefix}reviewsummary/game_id={id}/review_stats.parquet"
                                        key_rsm_processed_files.append(f"s3://{bucket}/{key_rsm}")
                                        s3.put_object(Bucket=bucket,Key=key_rsm,Body=parquet_buffer.getvalue())
                                
                                if user_review_json["reviews"]:
                                        reviews_df = pd.json_normalize(user_review_json["reviews"])
                                        reviews_df = reviews_df[["language","review"]]
                                        df_reviews = pd.concat([df_reviews,reviews_df],ignore_index=True)
                                
                                cursor = user_review_json.get("cursor","")
                                reviews_count +=100     
                                
                                if not cursor or reviews_count>500 or not user_review_json["reviews"]:
                                        break
                                else:
                                        params_reviews["cursor"]=cursor
                        ##Now save all the data into the bucket
                        if not df_reviews.empty:
                                parquet_buffer = BytesIO()
                                df_reviews.to_parquet(parquet_buffer,index=False)
                                key_reviews_detail = f"{prefix}reviews/game_id={id}/review.parquet" 
                                key_rev_processed_files.append(f"s3://{bucket}/{key_reviews_detail}")
                                s3.put_object(Bucket=bucket,Key=key_reviews_detail,Body=parquet_buffer.getvalue())

                except Exception as e:
                        print("Game not released",e)                
        return {
        "status": "success",
        "topsellers_s3": f"s3://{bucket}/{key_topsellers}",
        "reviewsummary_s3": key_rsm_processed_files,
        "reviewdetail_s3": key_rev_processed_files}