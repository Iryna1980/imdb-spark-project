from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
from pyspark.sql.functions import col


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

    df_ukr = title_akas_df.filter(col("region") == "UA")
    df_ukr.show()

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
    df_19th_century = name_basics_df.filter(col("birthYear") >= 1800).filter(col("birthYear") < 1900)
    df_19th_century.show()
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
    df_2_hours = title_basics_df.filter(col("runtimeMinutes") > 120)
    df_2_hours.show()
    df_2_hours.write.mode('overwrite').csv("results")

    #allowed_title_types = ["movie", "tvSeries", "tvMovie", "tvMiniSeries"]
    #title_basics_df_movies = title_basics_df.filter(col('titleType').isin(allowed_title_types))
    #joined_df = title_basics_df_movies.join(title_principals_df, on='tconst', how='inner')
    #character_df = joined_df.filter(col('job').isNotNull())
    #result_df = character_df.join(name_basics_df, on='nconst', how='inner')
    #result_df.select('primaryName', 'primaryTitle', 'characters').show()
    #result_df.select('primaryName', 'primaryTitle', 'characters').csv("result.csv", header=True)


if __name__ == '__main__':

    main()
 
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
