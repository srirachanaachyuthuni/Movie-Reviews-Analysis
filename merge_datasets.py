import sys
from os.path import isdir

import numpy as np
import pandas as pd
from pymongo import MongoClient

name_basics = "name.basics.tsv.gz"
title_basics = "title.basics.tsv.gz"
title_principals = "title.principals.tsv.gz"
ml_links = "links.csv"
ml_ratings = "ratings.csv"


def parse_argv(argv):
    if len(argv) != 3 and len(argv) != 4:
        print("Usage: python3 merge_datasets.py <directory containing the IMDb dataset>"
              " <directory containing the MovieLens dataset> [MongoDB connection string]")
        sys.exit(1)

    imdb_dir = argv[1]
    ml_dir = argv[2]
    mongodb_connection_string = None if len(argv) == 3 else argv[3]

    if not (isdir(imdb_dir) and isdir(ml_dir)):
        print("Invalid directory/directories")
        sys.exit(1)

    return imdb_dir, ml_dir, mongodb_connection_string


def get_imdb_gz_df(imdb_dir, imdb_file, columns_to_read):
    return pd.read_csv(imdb_dir + "\\" + imdb_file, sep="\t", usecols=columns_to_read, low_memory=False,
                       compression='gzip')


def get_ml_csv_df(ml_dir, ml_file, columns_to_read):
    return pd.read_csv(ml_dir + "\\" + ml_file, usecols=columns_to_read, low_memory=False)


def get_imdb_dfs(imdb_dir):
    name_basics_columns = ['nconst', 'primaryName', 'birthYear', 'deathYear']
    name_basics_df = get_imdb_gz_df(imdb_dir, name_basics, name_basics_columns)
    nconst_list = [int(nconst[2:]) for nconst in name_basics_df['nconst']]
    birth_year_list = [np.nan if birth_year == '\\N' else int(birth_year) for birth_year in name_basics_df['birthYear']]
    death_year_list = [np.nan if death_year == '\\N' else int(death_year) for death_year in name_basics_df['deathYear']]
    name_basics_df['nconst'] = nconst_list
    name_basics_df['birthYear'] = birth_year_list
    name_basics_df['deathYear'] = death_year_list
    name_basics_df.rename(columns={'nconst': 'id'}, inplace=True)

    title_basics_columns = ['tconst', 'titleType', 'primaryTitle', 'startYear', 'runtimeMinutes', 'genres']
    title_basics_df = get_imdb_gz_df(imdb_dir, title_basics, title_basics_columns)
    titles_to_drop = title_basics_df[~(title_basics_df['titleType'] == 'movie')
                                     & ~(title_basics_df['titleType'] == 'short')
                                     & ~(title_basics_df['titleType'] == 'tvMovie')
                                     & ~(title_basics_df['titleType'] == 'tvShort')].index
    title_basics_df.drop(titles_to_drop, inplace=True)
    title_basics_df.drop(['titleType'], axis=1, inplace=True)
    tconst_list = [int(tconst[2:]) for tconst in title_basics_df['tconst']]
    start_year_list = [np.nan if start_year == '\\N' else int(start_year)
                       for start_year in title_basics_df['startYear']]
    runtime_list = [np.nan if not str(runtime).isdigit() else int(runtime)
                    for runtime in title_basics_df['runtimeMinutes']]
    genres_list = [str(genres).split(',') for genres in title_basics_df['genres']]
    title_basics_df['tconst'] = tconst_list
    title_basics_df['startYear'] = start_year_list
    title_basics_df['runtimeMinutes'] = runtime_list
    title_basics_df['genres'] = genres_list
    title_basics_df.rename(columns={'tconst': 'id'}, inplace=True)

    title_principals_columns = ['tconst', 'nconst', 'category']
    title_principals_df = get_imdb_gz_df(imdb_dir, title_principals, title_principals_columns)
    nconst_list = [int(nconst[2:]) for nconst in title_principals_df['nconst']]
    tconst_list = [int(tconst[2:]) for tconst in title_principals_df['tconst']]
    title_principals_df['nconst'] = nconst_list
    title_principals_df['tconst'] = tconst_list
    title_principals_df.rename(columns={'tconst': 'movieId', 'nconst': 'personId'}, inplace=True)

    return name_basics_df, title_basics_df, title_principals_df


def get_ml_dfs(ml_dir):
    ml_links_columns = ['movieId', 'imdbId']
    ml_links_df = get_ml_csv_df(ml_dir, ml_links, ml_links_columns)
    ml_links_map = dict((link['movieId'], link['imdbId']) for link in ml_links_df.to_dict('records'))

    ml_ratings_columns = ['userId', 'movieId', 'rating', 'timestamp']
    ml_ratings_df = get_ml_csv_df(ml_dir, ml_ratings, ml_ratings_columns)
    imdb_movie_id_list = [ml_links_map[movie_id] for movie_id in ml_ratings_df['movieId']]
    ml_ratings_df['movieId'] = imdb_movie_id_list

    return ml_ratings_df


def create_collections(imdb_dir, ml_dir, mongodb_connection_string):
    name_basics_df, title_basics_df, title_principals_df = get_imdb_dfs(imdb_dir)

    client = MongoClient() if mongodb_connection_string is None else MongoClient(mongodb_connection_string)
    database = client['MapReduce']

    person_collection = database['Person']
    person_list = [dict((key, value) for key, value in zip(name_basics_df.columns, row)
                        if value is not np.nan and value == value) for row in name_basics_df.to_numpy()]
    person_collection.insert_many(person_list)

    movie_collection = database['Movie']
    movie_list = [dict((key, value) for key, value in zip(title_basics_df.columns, row)
                       if value is not np.nan and value == value) for row in title_basics_df.to_numpy()]
    movie_collection.insert_many(movie_list)

    person_role_collection = database['Person_Roles']
    person_role_collection.insert_many(title_principals_df.to_dict('records'))

    ml_ratings_df = get_ml_dfs(ml_dir)
    ratings_collection = database['Ratings']
    ratings_collection.insert_many(ml_ratings_df.to_dict('records'))


def main():
    imdb_dir, ml_dir, mongodb_connection_string = parse_argv(sys.argv)
    create_collections(imdb_dir, ml_dir, mongodb_connection_string)


if __name__ == '__main__':
    main()
