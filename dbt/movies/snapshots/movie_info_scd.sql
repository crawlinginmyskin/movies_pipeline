{% snapshot movie_info_scd %}
    {{
        config(
            target_schema='snapshots',  
            strategy='check',    
            unique_key='title',
            check_cols='all'
        )
    }}

select
    s.title,
    CAST(s.year as INTEGER) as year,
    s.release_date,
    s.genre,
    s.director,
    s.writer,
    s.language,
    s.country,
    s.plot
    from {{source("source", "stg_movie_info_dim")}} s
{% endsnapshot %}