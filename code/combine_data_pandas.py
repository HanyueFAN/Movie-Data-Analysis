#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#extract IMDb
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



# In[ ]:


#extract TMDb
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


# In[ ]:


#format IMDb data
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

convert_raw_to_formatted(file_name, current_day, data_entity)
convert_raw_to_formatted(file_name4, current_day, data_entity4)
convert_raw_to_formatted(file_name5, current_day, data_entity5)
convert_raw_to_formatted(file_name6, current_day, data_entity6)
convert_raw_to_formatted(file_name7, current_day, data_entity7)


# In[ ]:


#format TMDb data
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



# In[1]:


import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# In[28]:


import os
HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"
# Load formatted IMDb data with rating information
imdb_rating_folder = DATALAKE_ROOT_FOLDER + "formatted/imdb/MovieRating/"
imdb_rating_data = pd.read_parquet(imdb_rating_folder)

# Load formatted IMDb data with original title information
imdb_title_folder = DATALAKE_ROOT_FOLDER + "formatted/imdb/MovieBasic/"
imdb_title_data = pd.read_parquet(imdb_title_folder)

# Merge IMDb tables with rating and original title based on 'tconst'
imdb_rating_title_data = imdb_rating_data.merge(imdb_title_data, on='tconst', how='inner')
imdb_rating_title_data.head()


# In[29]:


# Load formatted TMDb data with original title information
tmdb_title_folder = DATALAKE_ROOT_FOLDER + "formatted/tmdb/MovieRating/"
tmdb_title_data = pd.read_parquet(tmdb_title_folder)
tmdb_rating_folder = DATALAKE_ROOT_FOLDER + "formatted/tmdb/MovieTopRated/"
tmdb_rating_data = pd.read_parquet(tmdb_title_folder)


# In[31]:


# combine TMDb tables
frames = [tmdb_title_data, tmdb_rating_data]
tmdb_rating_title_data =pd.concat(frames)
tmdb_rating_title_data.head()


# In[7]:


imdb_rating_title_data = imdb_rating_title_data[['averageRating', 'numVotes', 'primaryTitle','originalTitle']]
imdb_rating_title_data.head()


# In[32]:


#Rename the 'original_title' column in TMDb DataFrame to match IMDb's 'originalTitle'
tmdb_rating_title_data = tmdb_rating_title_data[['vote_average', 'vote_count', 'title','original_title']]
tmdb_rating_title_data.head()


# In[33]:


tmdb_rating_title_data = tmdb_rating_title_data.rename(columns={'original_title': 'originalTitle'})
tmdb_rating_title_data = tmdb_rating_title_data.rename(columns={'vote_average': 'averageRating'})
tmdb_rating_title_data = tmdb_rating_title_data.rename(columns={'vote_count': 'numVotes'})
tmdb_rating_title_data = tmdb_rating_title_data.rename(columns={'title': 'primaryTitle'})
tmdb_rating_title_data.head()


# In[48]:


frames2 = [imdb_rating_title_data,tmdb_rating_title_data]
combine_twoAPI = pd.concat(frames2)


# In[49]:


HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"
USAGE_OUTPUT_FOLDER_ALL = DATALAKE_ROOT_FOLDER +"usage/movieAll/MovieInfo/" + '20230610' + "/"
if not os.path.exists(USAGE_OUTPUT_FOLDER_ALL):
    os.makedirs(USAGE_OUTPUT_FOLDER_ALL)


# In[50]:


combine_twoAPI.to_parquet(USAGE_OUTPUT_FOLDER_ALL+'combine_twoAPI.snappy.parquet', compression='snappy')


# In[46]:


import os
from pyspark.sql import SQLContext
HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"

def combine_data(current_day):
    RATING_PATH = DATALAKE_ROOT_FOLDER + "formatted/imdb/MovieRating/" +current_day + "/"
    USAGE_OUTPUT_FOLDER_STATS = DATALAKE_ROOT_FOLDER +"usage/movieAnalysis/MovieStatistics/" + current_day + "/"
    USAGE_OUTPUT_FOLDER_BEST = DATALAKE_ROOT_FOLDER +"usage/movieAnalysis/MovieTop10/" + current_day + "/"
    if not os.path.exists(USAGE_OUTPUT_FOLDER_STATS):
        os.makedirs(USAGE_OUTPUT_FOLDER_STATS)
    if not os.path.exists(USAGE_OUTPUT_FOLDER_BEST):
        os.makedirs(USAGE_OUTPUT_FOLDER_BEST)
    from pyspark import SparkContext
    sc = SparkContext(appName="CombineData")
    sqlContext = SQLContext(sc)
    df_ratings = sqlContext.read.parquet(RATING_PATH)
    df_ratings.registerTempTable("ratings")
 # Check content of the DataFrame df_ratings:
    print(df_ratings.show())
    stats_df = sqlContext.sql("SELECT AVG(averageRating) AS avg_rating,"
                              "       MAX(averageRating) AS max_rating," " MIN(averageRating) AS min_rating,"
                              "       COUNT(averageRating) AS count_rating"
                              "       FROM ratings LIMIT 10")
    top10_df = sqlContext.sql("SELECT primaryTitle, averageRating"
                              " FROM ratings"
                              " WHERE numVotes > 5000 "
                              " ORDER BY averageRating DESC"
                              " LIMIT 10")
    print(stats_df.show())
 # Check content of the DataFrame stats_df and save it:
    stats_df.write.save(USAGE_OUTPUT_FOLDER_STATS + "res.snappy.parquet",mode="overwrite")
 # Check content of the DataFrame top10_df and save it:
    print(top10_df.show())
    stats_df.write.save(USAGE_OUTPUT_FOLDER_BEST + "res.snappy.parquet",mode="overwrite")

combine_data("20230610")



# In[51]:


from elasticsearch import Elasticsearch
import pandas as pd


# In[38]:


df5 = pd.read_parquet('combine_twoAPI.snappy.parquet')


# In[39]:


from elasticsearch import Elasticsearch
import pandas as pd
import ssl


# In[40]:


ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE


# In[41]:


import urllib3
urllib3.disable_warnings()


# In[42]:


es = Elasticsearch(
    hosts=[{'host': 'localhost', 'port': 9200}],
    http_auth=('elastic', 'sKzLFRnpvFbd2s6fhi_y'),
    scheme='https',
    ssl_context=ssl_context
)

es.info(pretty=True)


# In[43]:


# Convert the DataFrame to a list of dictionaries
data = combine_twoAPI.to_dict(orient='records')
# Index each document in Elasticsearch
for doc in data:
    # Filter documents based on averageRating and numVotes criteria
    if doc['averageRating'] >= 6.0 and doc['numVotes'] > 10000:
        # Index only the desired fields
        es.index(index='movies_index', body={
            'averageRating': doc['averageRating'],
            'numVotes': doc['numVotes'],
            'primaryTitle': doc['primaryTitle'],
            'originalTitle': doc['originalTitle']
        })

print("Data indexing completed.")


# In[ ]:




