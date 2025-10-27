import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
import boto3
import json

def handler(event,context):
    s3 = boto3.client('s3')
    #List the file directories you want to export to google sheet
    file1 = "s3://vgsa/gold/sentiment_analysis_result/" ##This file contains game name, reviews and sentiments of each reviews acquired by applying a multi-lingual sentiment analysis model
    file2 = "s3://vgsa/silver/silver_tables_dbt_result/game_reviews_summary/" ##This file contains stats about reviews stats
    #Then read these files in form of dataframe
    df_file1 = pd.read_parquet(file1)
    df_file2 = pd.read_parquet(file2)
    ##Establish Google sheet Creds
    secret_key_name = "google/service_account"
    region_name = "eu-central-1"
    client = boto3.client("secretsmanager")
    ##Get the secret
    response = client.get_secret_value(SecretId=secret_key_name)
    service_account_info = json.loads(response["SecretString"])
    #Load the creds
    scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
    client = gspread.authorize(creds)
    sheet1 = client.open("output").sheet1
    sheet1.clear()
    #Upload file 1 to google sheet
    set_with_dataframe(sheet1,df_file1) # File 1 contains information about each review and sentiment of the review per game
    sheet2 = client.open("output").get_worksheet(1)
    #Clear the sheet of any initial texts
    sheet2.clear()
    #Now write the second parquet file into it
    set_with_dataframe(sheet2, df_file2)
    return {
        'statusCode':200,
        'body': f"Uploaded from bucket to google sheet"
    }