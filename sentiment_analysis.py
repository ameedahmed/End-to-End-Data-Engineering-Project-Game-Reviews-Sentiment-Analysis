import boto3
import pandas as pd
import s3fs
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import os
from io import BytesIO
from huggingface_hub import InferenceClient
import boto3

s3 = boto3.client("s3")
def handler(event,context):
    ##Initiate the client to read the secret token from hugging face
    client = boto3.client("secretsmanager")
    ##Define the parameters for the bucket
    bucket = os.environ["vgsa"]
    prefix = "gold/"
    ##Define the pathway to read the silver files from (i.e. the table comprising of reviews per game for classification task)
    key = "silver/silver_tables_dbt_result/game_reviews"
    fs = s3fs.S3FileSystem()
    ##Read the file from aws s3 bucket
    df = pd.read_parquet(key,filesystem=fs)
    #Read the HF token
    secret_name = "hf_token"
    response = client.get_secret_value(SecretId=secret_name)
    hf_token = response["SecretString"]
    os.environ["HF_TOKEN"] = hf_token
    client = InferenceClient(provider="auto",api_key=os.environ["HF_TOKEN"])
    ##Load the multi-lingual classification model
    tokenizer = AutoTokenizer.from_pretrained("tabularisai/multilingual-sentiment-analysis")
    model = AutoModelForSequenceClassification.from_pretrained("tabularisai/multilingual-sentiment-analysis")
    ##Perform classification task for each game
    unique_games = df["name"].unique()
    empty_df = pd.DataFrame()
    for game in unique_games:
        game_df = df[df["name"]==game]
        game_df = game_df.reset_index(drop=True)
        #for each game cycle through the reviews row by row
        for i in range(len(game_df)):
            try:
                ##Limitation of the model is 512 tokens. Truncate the reviews where greater
                if len(game_df["review"].iloc[i])>512:
                    result = client.text_classification(game_df["review"].iloc[i][:512],model="tabularisai/multilingual-sentiment-analysis")
                else:
                    result = client.text_classification((game_df["review"].iloc[i]),model="tabularisai/multilingual-sentiment-analysis")
                max_result = max(result,key=lambda x: x["score"])
                game_df.loc[i,"sentiment"] = max_result["label"]
            except Exception as e:
                print(game_df.iloc[i])
                print("Error",e)
        ##Once the rows have been populated, append this dataframe to the empty dataframe
        empty_df = pd.concat([empty_df,game_df],ignore_index=True)
        
        parquet_buffer = BytesIO()
        
        empty_df.to_parquet(parquet_buffer,index=False)
        
        key_topsellers = f"{prefix}sentiment_analysis_table.parquet"
        
        s3.put_object(Bucket=bucket,Key=key_topsellers, Body = parquet_buffer.getvalue())