import os
import pandas as pd

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def convert_raw_to_formatted(file_name, current_day):
    # RAW_PATH = DATALAKE_ROOT_FOLDER + "raw/tmdb/MovieRating/" + current_day + "/" + file_name
    # FORMATTED_FOLDER = DATALAKE_ROOT_FOLDER + "formatted/tmdb/MovieRating/" + current_day + "/"

    RAW_PATH = DATALAKE_ROOT_FOLDER + "raw/tmdb/MovieTopRated/" + current_day + "/" + file_name
    FORMATTED_FOLDER = DATALAKE_ROOT_FOLDER + "formatted/tmdb/MovieTopRated/" + current_day + "/"
    if not os.path.exists(FORMATTED_FOLDER):
        os.makedirs(FORMATTED_FOLDER)

    df = pd.read_json(RAW_PATH)
    parquet_file_name = file_name.replace(".json", ".snappy.parquet")
    df.to_parquet(FORMATTED_FOLDER + parquet_file_name, compression='snappy')


current_day = "20230610"
file_name = "movies.json"
# convert_raw_to_formatted(file_name, current_day)

file_name2 = "movies_toprated.json"
convert_raw_to_formatted(file_name2, current_day)

