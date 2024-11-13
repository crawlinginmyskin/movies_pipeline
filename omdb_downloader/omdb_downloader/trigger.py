import pandas as pd
from google.cloud import secretmanager
from google.cloud import storage
from io import BytesIO
import requests
import os

URL = "http://www.omdbapi.com/?t={title}&apikey={key}"


def get_titles(n=10):
    client = storage.Client(project=os.getenv("PROJECT_ID"))
    bucket = client.get_bucket(os.getenv("BUCKET"))
    blob = bucket.blob("movie_titles.csv")
    data = blob.download_as_bytes()
    df = pd.read_csv(BytesIO(data))
    return df['title'].sample(n).to_list()

def access_secret(project_id, secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")

def parse_rating(rating: str) -> int:
    # making ratings consistent - every rating will be parsed as a value between 0-100
    if r'/' in rating:
        rating = rating.split('/')[0]
    if '.' in rating:
        rating = rating.replace('.', '')
    if '%' in rating:
        rating = rating.replace('%', '')
    return int(rating)

def logic():
    created_date = pd.Timestamp('now').strftime('%Y-%m-%d')
    bucket = os.getenv("BUCKET")
    titles = get_titles(20)
    project_id = 'GCP_PROJECT_ID'
    secret_id = 'OMDB_API_KEY'
    api_key = access_secret(project_id=project_id, secret_id=secret_id).lstrip(" ").rstrip(" ")
    fact_df_list = []
    dim_ratings_list = []
    dim_movie_info_list = []
    for title in titles:
        response = requests.get(URL.format(key=api_key, title=title))
        if response.status_code == 200:
            data = response.json()
            print(title)
            try:
                fact_movie_metrics = {
                    "movie_title": [data["Title"]],
                    "movie_year": [int(data["Year"])],
                    "box_office": [int(data["BoxOffice"].replace("$", "").replace(",", "")) if data["BoxOffice"] != "N/A" else None],
                    "download_date": [created_date]
                }
                dim_movie_info = {
                    "title": [data["Title"]],
                    "year": [int(data["Year"])],
                    "release_date": [pd.Timestamp((data["Released"])).strftime('%Y-%m-%d')],
                    "genre": [data["Genre"]],
                    "director": [data["Director"]],
                    "writer": [data["Writer"]],
                    "country": [data["Country"]],
                }

                dim_ratings = [{"movie_title": data["Title"],
                                "movie_year": int(data["Year"]),
                                "download_date": created_date, 
                                "rating_source": r["Source"], 
                                "rating_value": parse_rating(r["Value"])} for r in data["Ratings"]
                            ]
            except KeyError:
                print(f'failed to parse {title}')
                continue
            fact_df = pd.DataFrame(fact_movie_metrics)
            dim_info_df = pd.DataFrame(dim_movie_info)
            dim_ratings_df = pd.DataFrame(dim_ratings)
            fact_df_list.append(fact_df)
            dim_movie_info_list.append(dim_info_df)
            dim_ratings_list.append(dim_ratings_df)
    filepath = 'gs://{bucket}/{date}/{filename}'
    fact_df_to_save = pd.concat(fact_df_list)
    dim_info_df_to_save = pd.concat(dim_movie_info_list)
    dim_ratings_to_save = pd.concat(dim_ratings_list)
    fact_df_to_save.to_csv(filepath.format(bucket = bucket, date=created_date, filename="movie_fact.csv"), index=False)
    dim_info_df_to_save.to_csv(filepath.format(bucket = bucket, date=created_date, filename="movie_info_dim.csv"), index=False)
    dim_ratings_to_save.to_csv(filepath.format(bucket = bucket, date=created_date, filename="ratings_dim.csv"), index=False)




if __name__ == "__main__":
    logic()
