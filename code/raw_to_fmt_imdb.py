import os
import pandas as pd

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def convert_raw_to_formatted(file_name, current_day, data_entity):
    RATING_PATH = DATALAKE_ROOT_FOLDER + f"raw/imdb/{data_entity}/{current_day}/{file_name}"
    FORMATTED_RATING_FOLDER = DATALAKE_ROOT_FOLDER + f"formatted/imdb/{data_entity}/{current_day}/"
    print("000")
    if not os.path.exists(FORMATTED_RATING_FOLDER):
        os.makedirs(FORMATTED_RATING_FOLDER)

    # Specify column types to handle mixed types warning
    # for MovieBasic
    # dtype = {'isOriginalTitle': str}

    #for MovieCrew
    # dtype = {'isAdult': str}
    df = pd.read_csv(RATING_PATH, sep='\t')
    print("111")
    parquet_file_name = file_name.replace(".tsv.gz", ".snappy.parquet")
    print("222")
    df.to_parquet(FORMATTED_RATING_FOLDER + parquet_file_name)
    print("done")

file_name = "title.akas.tsv.gz"
current_day = "20230610"
data_entity = "MovieAkas"

file_name4 = "title.basics.tsv.gz"
data_entity4 = "MovieBasic"

file_name5 = "title.crew.tsv.gz"
data_entity5 = "MovieCrew"

file_name6 = "title.episode.tsv.gz"
data_entity6 = "MovieEpisode"

file_name7 = "title.principals.tsv.gz"
data_entity7 = "MoviePrincipals"

# convert_raw_to_formatted(file_name, current_day, data_entity)
# convert_raw_to_formatted(file_name4, current_day, data_entity4)
# convert_raw_to_formatted(file_name5, current_day, data_entity5)
# convert_raw_to_formatted(file_name6, current_day, data_entity6)
convert_raw_to_formatted(file_name7, current_day, data_entity7)


