import sys
import csv
from pyspark.sql import SQLContext
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession, functions as F
import pyspark.sql.types as t
import py4j
from pyspark.sql import readwriter
from pyspark.sql import Window, types
from pyspark.sql.functions import floor
from pyspark.sql.functions import col, count, desc, dense_rank, explode, split, collect_list, expr
from pyspark.sql.functions import collect_list, struct, desc
from pyspark.sql.types import *


def main():
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("imdb-spark-project app")
                     .config(conf=SparkConf())
                     .getOrCreate())

    title_akas_schema = t.StructType([t.StructField('titleId', t.StringType(), False),
                                      t.StructField('ordering', t.IntegerType(), False),
                                      t.StructField('title', t.StringType(), True),
                                      t.StructField('region', t.StringType(), True),
                                      t.StructField('language', t.StringType(), True),
                                      t.StructField('types', t.StringType(), True),
                                      t.StructField('attributes', t.StringType(), True),
                                      t.StructField('isOriginalTitle', t.BooleanType(), True)])

    path_akas = r'imdb-data\title.akas.tsv.gz'
    title_akas_df = spark_session.read.csv(path_akas, sep=r'\t', header=True, nullValue='null', schema=title_akas_schema)
    title_akas_df.show()
    title_akas_df.printSchema()



    title_crew_schema = t.StructType([t.StructField('tconst', t.StringType(), False),
                                      t.StructField('directors', t.StringType(), False),
                                      t.StructField('writers', t.StringType(), True)])
    path_crew = r'imdb-data\title.crew.tsv.gz'
    title_crew_df = spark_session.read.csv(path_crew, sep=r'\t', header=True, nullValue='null', schema=title_crew_schema)
    title_crew_df.show()
    title_crew_df.printSchema()
    name_basics_schema = t.StructType([t.StructField('nconst', t.StringType(), False),
                                      t.StructField('primaryName', t.StringType(), False),
                                      t.StructField('birthYear', t.IntegerType(), True),
                                      t.StructField('deathYear', t.IntegerType(), True),
                                      t.StructField('primaryProfession', t.StringType(), True),
                                      t.StructField('knownForTitles', t.StringType(), True)])
    path_name_basics = r'imdb-data\name.basics.tsv.gz'
    name_basics_df = spark_session.read.csv(path_name_basics, sep=r'\t', header=True, nullValue='null', schema=name_basics_schema)
    name_basics_df.show()
    name_basics_df.printSchema()

    title_episode_schema = t.StructType([t.StructField('tconst', t.StringType(), False),
                                      t.StructField('parentTconst', t.StringType(), False),
                                      t.StructField('seasonNumber', t.IntegerType(), True),
                                         t.StructField('episodeNumber', t.IntegerType(), True)])
    path_episode = r'imdb-data\title.episode.tsv.gz'
    title_episode_df = spark_session.read.csv(path_episode, sep=r'\t', header=True, nullValue='null', schema=title_episode_schema)
    title_episode_df.show()
    title_episode_df.printSchema()
    title_principals_schema = t.StructType([t.StructField('tconst', t.StringType(), False),
                                         t.StructField('ordering', t.IntegerType(), False),
                                         t.StructField('nconst', t.StringType(), True),
                                         t.StructField('category', t.StringType(), True),
                                         t.StructField('job', t.StringType(), True),
                                         t.StructField('characters', t.StringType(), True)])

    path_principals = r'imdb-data\title.principals.tsv.gz'
    title_principals_df = spark_session.read.csv(path_principals, sep=r'\t', header=True, nullValue='null', schema=title_principals_schema)

    title_principals_df.show()
    title_principals_df.printSchema()
    title_ratings_schema = t.StructType([t.StructField('tconst', t.StringType(), False),
                                            t.StructField('averageRating', t.DoubleType(), False),
                                            t.StructField('numVotes', t.IntegerType(), True)])
    path_ratings = r'imdb-data\title.ratings.tsv.gz'
    title_ratings_df = spark_session.read.csv(path_ratings, sep=r'\t', header=True, nullValue='null',schema=title_ratings_schema)
    title_ratings_df.show()
    title_ratings_df.printSchema()
    title_basics_schema = t.StructType([t.StructField('tconst', t.StringType(), False),
                                            t.StructField('titleType', t.StringType(), False),
                                            t.StructField('primaryTitle', t.StringType(), True),
                                            t.StructField('originalTitle', t.StringType(), True),
                                            t.StructField('isAdult', t.StringType(), True),
                                            t.StructField('startYear', t.IntegerType(), True),
                                        t.StructField('endYear', t.IntegerType(), True),
                                       t.StructField('runtimeMinutes', t.IntegerType(), True),
                                        t.StructField('genres', t.StringType(), True)])

    path_title_basics = r'imdb-data\title.basics.tsv.gz'
    title_basics_df = spark_session.read.csv(path_title_basics, sep=r'\t', header=True, nullValue='null', schema=title_basics_schema)
    title_basics_df.show()
    title_basics_df.printSchema()


    #task1
    df_ukr = title_akas_df.filter(col("region") == "UA")
    df_ukr.show()
    path_to_save1 = 'imdb-data/results/task1'
    df_ukr.write.csv(path_to_save1, header=True, mode="overwrite")
    #task2
    df_19th_century = name_basics_df.filter(col("birthYear") >= 1800).filter(col("birthYear") < 1900)
    df_19th_century.show()
    path_to_save2 = 'imdb-data/results/task2'
    df_19th_century.write.csv(path_to_save2, header=True, mode="overwrite")
    #task3
    df_2_hours = title_basics_df.filter(col("runtimeMinutes") > 120)
    df_2_hours.show()
    path_to_save3 = 'imdb-data/results/task3'
    df_2_hours.write.csv(path_to_save3, header=True, mode="overwrite")
    #task4
    allowed_title_types = ["movie", "tvSeries", "tvMovie", "tvMiniSeries"]
    title_basics_df_movies = title_basics_df.filter(col('titleType').isin(allowed_title_types))
    joined_df = title_basics_df_movies.join(title_principals_df, on='tconst', how='inner')
    character_df = joined_df.filter(col('job').isNotNull())
    result_df = character_df.join(name_basics_df, on='nconst', how='inner')
    result_df.select('primaryName', 'primaryTitle', 'characters').show()
    path_to_save4 = 'imdb-data/results/task4'
    result_df.select('primaryName', 'primaryTitle', 'characters').write.csv(path_to_save4,header=True, mode="overwrite")
    #task5
    adult_titles_df = title_basics_df.join(title_akas_df, title_basics_df.tconst == title_akas_df.titleId, 'inner') \
        .filter(title_basics_df.isAdult == '1') \
        .groupBy(title_akas_df.region) \
        .agg(count('*').alias('count'))
    top_100_adult_regions_df = adult_titles_df.orderBy(adult_titles_df['count'].desc()).limit(100)
    top_100_adult_regions_df.show()
    path_to_save5 = 'imdb-data/results/task5'
    top_100_adult_regions_df.write.csv(path_to_save5, header=True, mode="overwrite")
    # task6
    tv_series_df = title_basics_df.join(title_episode_df, title_basics_df['tconst'] == title_episode_df['parentTconst'])
    tv_series_df.show()
    episode_count_df = tv_series_df.groupBy('parentTconst').count()
    episode_count_df.show()
    top_episodes_df = episode_count_df.orderBy(desc('count')).limit(50)
    top_episodes_df.show()
    path_to_save6 = 'imdb-data/results/task6'
    top_episodes_df.write.csv(path_to_save6, header=True, mode="overwrite")
    # task7
    title_basics_df = title_basics_df.withColumn('decade', floor(title_basics_df.startYear / 10) * 10)
    joined_df = title_basics_df.join(title_ratings_df, 'tconst', 'left')
    grouped_df = joined_df.groupBy('decade', 'primaryTitle').agg({'averageRating': 'mean', 'numVotes': 'sum'})
    sorted_df = grouped_df.sort(['decade', 'avg(averageRating)'], ascending=[1, 0])
    w = Window.partitionBy('decade').orderBy(grouped_df['avg(averageRating)'].desc())
    ranked_df = sorted_df.withColumn('rank', dense_rank().over(w))
    result_df = ranked_df.filter(ranked_df['rank'] <= 10)
    result_df.show()
    path_to_save7 = 'imdb-data/results/task7'
    result_df.write.csv(path_to_save7, header=True, mode="overwrite")
    # task8
    joined_df = title_basics_df.join(title_ratings_df, 'tconst')
    grouped_df = joined_df.groupBy('genres').agg(F.avg('averageRating').alias('avg_rating'),
                                               F.sum('numVotes').alias('total_votes')).orderBy(F.desc('avg_rating'),
                                                                                               F.desc('total_votes'))
    w = Window.partitionBy('genres').orderBy(F.desc('avg_rating'), F.desc('total_votes'))
    ranked_df = grouped_df.select('genres', 'avg_rating', 'total_votes', F.row_number().over(w).alias('rank'))
    top_10_df = ranked_df.filter('rank <= 10')
    top_10_df.show()
    path_to_save8 = 'imdb-data/results/task8'
    top_10_df.write.csv(path_to_save8, header=True, mode="overwrite")








if __name__ == '__main__':

    main()
 
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
