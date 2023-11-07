import os
from datetime import date
import requests

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def fetch_data_from_imdb(url, data_entity_name, **kwargs):
    current_day = date.today().strftime("%Y%m%d")
    TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/" + data_entity_name + "/" + current_day + "/"

    if not os.path.exists(TARGET_PATH):
        os.makedirs(TARGET_PATH)

    print("Fetching data from IMDB...")
    r = requests.get(url, allow_redirects=True)
    print("Data fetched successfully.")

    file_name = url.split("/")[-1]
    file_path = os.path.join(TARGET_PATH, file_name)

    print("Saving data...")
    open(file_path, 'wb').write(r.content)
    print("Data saved successfully.")

    print("Process completed.")


imdb_url = 'https://datasets.imdbws.com/title.ratings.tsv.gz'
data_entity = 'imdb/MovieRating'

imdb_url2 = 'https://datasets.imdbws.com/name.basics.tsv.gz'
data_entity2 = 'imdb/MovieName'


imdb_url3 = 'https://datasets.imdbws.com/title.akas.tsv.gz'
data_entity3 = 'imdb/MovieAkas'

imdb_url4 = 'https://datasets.imdbws.com/title.basics.tsv.gz'
data_entity4 = 'imdb/MovieBasic'

imdb_url5 = 'https://datasets.imdbws.com/title.crew.tsv.gz'
data_entity5 = 'imdb/MovieCrew'

imdb_url6 = 'https://datasets.imdbws.com/title.episode.tsv.gz'
data_entity6 = 'imdb/MovieEpisode'

imdb_url7 = 'https://datasets.imdbws.com/title.principals.tsv.gz'
data_entity7 = 'imdb/MoviePrincipals'

fetch_data_from_imdb(imdb_url, data_entity)
fetch_data_from_imdb(imdb_url2, data_entity2)
fetch_data_from_imdb(imdb_url3, data_entity3)
fetch_data_from_imdb(imdb_url4, data_entity4)
fetch_data_from_imdb(imdb_url5, data_entity5)
fetch_data_from_imdb(imdb_url6, data_entity6)
fetch_data_from_imdb(imdb_url7, data_entity7)

