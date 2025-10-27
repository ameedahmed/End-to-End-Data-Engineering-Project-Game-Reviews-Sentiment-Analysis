from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
import subprocess
from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash import BashOperator
#
aws_conn = BaseHook.get_connection("aws_default")
##First create the lambda client so as to establish connection with aws lambda
aws_access_key = aws_conn.login
aws_access_pwd = aws_conn.password
#Pushing to xcom so other tasks can use it as well

lambda_client = boto3.client("lambda",aws_access_key_id = aws_access_key,aws_secret_access_key = aws_access_pwd,region_name="eu-central-1")

#Then def#ine a function to invoke the created lambdas for silver and golden layers
#def run_lambda(function_name):
#   response = lambda_client.invoke(FunctionName=function_name)
#  print(res#ponse)

def run_lambda(function_name):
        print(f"Invoking Lambda function: {function_name}")
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse'
        )
        print("Lambda response:", response)
        payload = response['Payload'].read().decode('utf-8')
        print("Lambda output:", payload)


####
#Now define the structure of your DAG
with DAG(dag_id = "game_sentiment_data_pipeline", start_date = datetime(2025,10,14), schedule_interval = "@monthly", catchup = False) as dag:
    ##First draw the operator for ingesting the list of top selling games and their reviews (both are included in docker_folder_files->bronze_layer->bronze_lambda.py)
    bronze_ingest_games_rvws = PythonOperator(task_id = "bronze_lambda_gms_rvws",python_callable=run_lambda,op_args=["bronze_lambda_with_docker"])
    #After designing the op#erator which will fetch you the list of top selling games and their reviews, schedule the function that fetches each of the game along with their IDs
    bronze_ingest_games_ids = PythonOperator(task_id = "bronze_lambda_gm_ids",python_callable = run_lambda, op_args=["bronze_game_id_name_docker"])
    #After you have the raw data, run the silver layer, to join the game ids with their games & reviews 
    
    #First join the games id and t#heir rvws smry with the name of the games.
    dbt_models_top_selling_games_id = BashOperator(
    task_id="dbt_top_selling_games_id",
    bash_command='cd /opt/airflow/silver_layer && /home/airflow/.local/bin/dbt run --select top_selling_games_id --full-refresh',  # Removed dbt debug
    env={
        'AWS_ACCESS_KEY_ID': aws_access_key,
        'AWS_SECRET_ACCESS_KEY': aws_access_pwd,
        'AWS_DEFAULT_REGION': 'eu-central-1'  # Added region
    })
    
    dbt_models_top_selling_games_reviews = BashOperator(
        task_id="dbt_run_top_selling_games_reviews",
        bash_command='cd /opt/airflow/silver_layer && /home/airflow/.local/bin/dbt run --select top_selling_games_reviews --full-refresh',  # Removed dbt debug
        env={
            'AWS_ACCESS_KEY_ID': aws_access_key,
            'AWS_SECRET_ACCESS_KEY': aws_access_pwd,
            'AWS_DEFAULT_REGION': 'eu-central-1'  # Added region
        }
    )
    
    dbt_models_top_selling_games_reviews_summary = BashOperator(
        task_id="dbt_run_top_selling_game_reviews_summary",
        bash_command='cd /opt/airflow/silver_layer && /home/airflow/.local/bin/dbt run --select game_reviews_summary --full-refresh',  # Removed dbt debug
        env={
            'AWS_ACCESS_KEY_ID': aws_access_key,
            'AWS_SECRET_ACCESS_KEY': aws_access_pwd,
            'AWS_DEFAULT_REGION': 'eu-central-1'  # Added region
        }
    )
    
    #Once the data is ready, execute the sentiment analysis script
    sentiment_analysis = PythonOperator(task_id="sent_analysis",python_callable=run_lambda,op_args=["sentiment_ntbk"])
    #After carrying out sentiment analysis, transfer the data to google sheet for looker to work on
    data_transfer = PythonOperator(task_id="DT",python_callable=run_lambda,op_args=["DataTransfer"])
    #Once all the scripts have been scheduled, set the flow
    bronze_ingest_games_rvws>>bronze_ingest_games_ids>>dbt_models_top_selling_games_id>>dbt_models_top_selling_games_reviews>>dbt_models_top_selling_games_reviews_summary>>sentiment_analysis>>data_transfer
    