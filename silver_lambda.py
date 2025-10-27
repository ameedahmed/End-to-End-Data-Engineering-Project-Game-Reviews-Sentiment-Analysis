import requests
import pandas as pd
import numpy as np
import json
import json_normalize
import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, IntegerType

#Extract the ids of the top 100 most sold games
merged_df = pd.merge(search_results_df,app_id_df,on="name",how="inner")
merged_df = merged_df.drop_duplicates(subset="name")
merged_df.head()

##Now fetch the reviews for these games:
#In one dataframe record the query summary of each game id

df_query_summary = pd.DataFrame()
review_df = pd.DataFrame()                             
#def get_review(review_app_id,params):
for id in merged_df["appid"]:
    params_reviews = {"json":1,"cursor":"*","num_per_page":100,"filter":"recent"}
    try:
        reviews_count = 100
        while True:
            user_review_url = f'https://store.steampowered.com/appreviews/{id}'
            print(id)
            print(params_reviews)
            request_review = requests.get(user_review_url,params = params_reviews)
            user_review_json = request_review.json()
            print(user_review_json)
            query_summary_json_df = pd.json_normalize(user_review_json["query_summary"])
            query_summary_json_df["game_id"] = id
            df_query_summary = pd.concat([df_query_summary,query_summary_json_df],ignore_index=True)
            reviews_json_df =  pd.json_normalize(user_review_json["reviews"])
            reviews_json_df = reviews_json_df[["language","review"]]
            reviews_json_df["game_id"] = id
            review_df = pd.concat([review_df,reviews_json_df],ignore_index=True)
            reviews_count += 100
            try:
                cursor = user_review_json["cursor"]
                print("To the next page. Next Page Cursor:", cursor)
            except Exception as e:
                print("No cursor available, setting it to empty")
                cursor = ''
            if not cursor or reviews_count>3000:
                break
            else:
                params_reviews["cursor"]=cursor                
    except Exception as e:
        print(e)
        print(id)
        print("Game not released")
df_query_summary.dropna(inplace=True)        
df_query_summary = df_query_summary[["review_score_desc","total_positive","total_negative","total_reviews","game_id"]]
