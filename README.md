# movies_pipeline
Movies Pipeline personal project

This pipeline focuses on 3 main technologies:
- Python and Docker/Cloud Run - simple script for downloading data from omdb API
- Airflow / Composer - simple set up for task orchestration
- dbt - set up as a data warehouse (with movies_info dim set up as SCD because i got some data initially of about ~6500 movies and want to fill it over time)

The scripts have been anonymized so that no data is leaked about my personal gcp project