--This is the last model that needs to be executed
-- 1. top_selling_games_names_id 2. top_selling_games_reviews 3. game_reviews_summary
{{ config(
    materialized = 'table',
    incremental_strategy = "delete+insert",
    unique_key = 'game_id',
    file_format = 'parquet',
    external_location = 's3://vgsa/silver/silver_tables_dbt_result/game_reviews_summary/') 
}}

select distinct cast(gi.name as varchar) as game_name, 
cast(rs.game_id as varchar) as game_id,
cast(rs.review_score as varchar) as review_score, 
cast(rs.review_score_desc as varchar) as review_score_desc, 
cast(rs.total_positive as varchar) as total_positive, 
cast(rs.total_negative as varchar) as total_negative, 
cast(rs.total_reviews as varchar) as total_reviews
from {{ source('gsa_bronze_tables', 'reviewsummary') }} as rs INNER JOIN {{ source('gsa_silver_tables', 'top_selling_games_names_id') }} as gi 
on cast(rs.game_id as varchar) = cast(gi.appid as varchar);