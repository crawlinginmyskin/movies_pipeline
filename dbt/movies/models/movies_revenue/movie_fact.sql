{{config(
    materialized="incremental",
    partition_by={"field": "download_date", "data_type": "DATE", "granularity": "MONTH"},
    cluster_by=["movie_id"]
)}}

select
row_number() over() as id,
i.dbt_scd_id as movie_id,
m.box_office,
d.date_id
from {{source("source", "stg_movie_fact")}} m
left join {{ref("movie_info_scd")}} i on 
m.movie_title = i.title
left join {{souce("prod_seeds", "date_dim")}} d
on m.download_date = d.date
where i.dbt_valid_to is NULL
{% if is_incremental() %}

and download_date > (select max(download_date) from {{ this }})

{% endif %}
