import boto3
import pandas as pd
import s3fs
import os
from huggingface_hub import InferenceClient

def handler(event,content):
  s3 = boto3.client("s3")
  client_secret = boto3.client("secretsmanager")
  fs = s3fs.S3FileSystem()

  bucket_name = "vgsa"
  file_path = "vgsa/silver/silver_tables_dbt_result/game_reviews/"
  df = pd.read_parquet(file_path,filesystem=fs)

  #Read the hugginface token
  secret_name = "hf_token"
  response = client_secret.get_secret_value(SecretId=secret_name)
  hf_token = response["SecretString"]
  os.environ["HF_TOKEN"] = hf_token
  client = InferenceClient(provider="auto",api_key=os.environ["HF_TOKEN"])
  # Load model
  batch_size=100
  df["review_new"] = df["review"].apply(lambda x: x[:512] if len(x)>512 else x)
  ##Now perform classification in batches
  for a in range(0, len(df), batch_size):
    batch = df["review_new"].iloc[a:a+batch_size].tolist()
    results = client.text_classification(batch, model="tabularisai/multilingual-sentiment-analysis")
    results_label = [i["label"] for i in results]
    df.loc[a:a+batch_size-1,"sentiment"] = results_label
  ##Once the whole df has been processed, save it to the below path
  output_path = "s3://vgsa/gold/sentiment_analysis_result/sentiment_analysis.parquet"
  df.to_parquet(output_path,filesystem=fs,index=False)