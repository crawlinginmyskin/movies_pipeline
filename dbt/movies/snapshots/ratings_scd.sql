{% snapshot ratings_scd %}
    {{
        config(
            target_schema='snapshots',  
            strategy='timestamp',  
            updated_at='download_date',  
            unique_key="CONCAT(movie_id, '-', rating_source)",
        )
    }}

    select
        m.dbt_scd_id as movie_id,
        r.rating_source,
        r.rating_value,
        r.download_date,
    from {{ source('source', 'stg_ratings_dim') }} r
    left join {{ref("movie_info_scd")}} m
    on r.movie_title = m.title
    where m.dbt_valid_to is null

{% endsnapshot %}