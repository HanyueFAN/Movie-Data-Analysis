import requests
import json
from datetime import date
import os

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"

def fetch_data_from_tmdb(api_key, **kwargs):
    movies = query_data_from_tmdb(api_key)
    store_tmdb_data(movies)

def query_data_from_tmdb(api_key):
    url = "https://api.themoviedb.org/3/movie/top_rated"
    # url = "https://api.themoviedb.org/3/movie/popular"
    params = {
        "api_key": api_key
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        return data["results"]
    else:
        print("Failed to fetch data from TMDb API.")
        return []

def store_tmdb_data(movies):
    current_day = date.today().strftime("%Y%m%d")
    TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/tmdb/MovieTopRated/" + current_day + "/"
    # TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/tmdb/MovieRating/" + current_day + "/"
    if not os.path.exists(TARGET_PATH):
        os.makedirs(TARGET_PATH)
    print("Writing here: ", TARGET_PATH)
    with open(TARGET_PATH + "movies_toprated.json", "w+") as f:
        json.dump(movies, f, indent=4)
    # with open(TARGET_PATH + "movies.json", "w+") as f:
    #     json.dump(movies, f, indent=4)


tmdb_api_key = "221b58836372efa0d61bc571ddb0911a"
fetch_data_from_tmdb(tmdb_api_key)
