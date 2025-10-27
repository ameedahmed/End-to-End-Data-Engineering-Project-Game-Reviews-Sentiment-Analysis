--First take the data from the model top_selling_games_names_id then run this function
-- 1. top_selling_games_names_id 2. top_selling_games_reviews 3. game_reviews_summary
--The objective of this model is to take the game id, name and reviews and join  
--Table Configuration
{{ 
  config(
    materialized = 'table',
    incremental_strategy = "delete+insert",
    unique_key = 'game_id',
    file_format = 'parquet',
    external_location = 's3://vgsa/silver/silver_tables_dbt_result/game_reviews/'
  ) 
}}

select distinct dr.game_id,tsd.name,dr.review
from {{source('gsa_silver_tables','top_selling_games_names_id')}} AS tsd 
INNER JOIN {{source('gsa_bronze_tables','reviews')}} AS dr 
ON CAST(tsd.appid AS varchar)=CAST(dr.game_id AS varchar);