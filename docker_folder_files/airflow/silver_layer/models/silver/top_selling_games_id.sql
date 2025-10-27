--Table Configuration
---- 1. top_selling_games_names_id 2. top_selling_games_reviews 3. game_reviews_summary
{{ 
  config(
    materialized = 'table',
    incremental_strategy = "delete+insert",
    unique_key = 'game_id',
    file_format = 'parquet',
    external_location = 's3://vgsa/silver/silver_tables_dbt_result/top_selling_games_names_id/'
  ) 
}}

SELECT distinct ts.name,ts.logo, gid.appid 
from {{source('gsa_bronze_tables','top_selling_games')}} AS ts 
INNER JOIN {{source('gsa_bronze_tables','game_id')}} AS gid 
ON ts.name=gid.name;