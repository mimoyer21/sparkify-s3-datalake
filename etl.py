import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import *


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['aws']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['aws']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.hadoop.fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID']) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY']) \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # define schema
    song_schema = StructType([ \
        StructField("num_songs",IntegerType()), \
        StructField("artist_id",StringType()), \
        StructField("artist_latitude",DoubleType()), \
        StructField("artist_longitude", DoubleType()), \
        StructField("artist_location", StringType()), \
        StructField("artist_name", StringType()), \
        StructField("song_id", StringType()), \
        StructField("title", StringType()), \
        StructField("duration", DoubleType()), \
        StructField("year", IntegerType()) \
    ])
    
    # read song data file
    df = spark.read.schema(song_schema).json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").mode('overwrite').parquet(output_data + "songs")

    # extract columns to create artists table (and rename cols to desired names)
    artists_col_names = ["artist_id", "name", "location", "latitude", "longitude"]
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").distinct().toDF(*artists_col_names) 
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + "artists")

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data"

    # define schema
    log_data_schema = StructType([ \
        StructField("artist",StringType()), \
        StructField("auth",StringType()), \
        StructField("firstName",StringType()), \
        StructField("gender", StringType()), \
        StructField("itemInSession", IntegerType()), \
        StructField("lastName", StringType()), \
        StructField("length", DoubleType()), \
        StructField("level", StringType()), \
        StructField("location", StringType()), \
        StructField("method", StringType()), \
        StructField("page", StringType()), \
        StructField("registration", DoubleType()), \
        StructField("sessionId", IntegerType()), \
        StructField("song", StringType()), \
        StructField("status", IntegerType()), \
        StructField("ts", LongType()), \
        StructField("userAgent", StringType()), \
        StructField("userId", StringType()) \
    ])
    
    # read log data file
    df = spark.read.schema(log_data_schema).json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page = 'NextSong'")
    df.createOrReplaceTempView("log_data")

    # extract columns for users table    
    users_col_names = ["user_id", "first_name", "last_name", "gender", "level"]
    users_table = df.select("userId", "firstName", "lastName", "gender", "level").distinct().toDF(*users_col_names)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + "users")
    
    # extract columns to create time table: start_time, hour, day, week, month, year, weekday
    time_table = spark.sql("""
        with timestamps as (
        select distinct ts
            , from_unixtime(ts / 1000) as start_time
        from log_data
        )
        select ts
            , start_time
            , extract(hour from start_time) as hour
            , extract(day from start_time) as day
            , extract(week from start_time) as week
            , extract(month from start_time) as month
            , extract(year from start_time) as year
            , extract(dayofweek from start_time) as weekday
        from timestamps
    """)
        
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").mode('overwrite').parquet(output_data + "time")

    # read in song and artist data to use for songplays table
    song_schema = StructType([ \
        StructField("song_id",StringType()), \
        StructField("title",StringType()), \
        StructField("artist_id",StringType()), \
        StructField("year", IntegerType()), \
        StructField("duration", DoubleType())
    ])
    song_data = output_data + "songs"
    song_df = spark.read.schema(song_schema).parquet(song_data)
    song_df.createOrReplaceTempView("songs")
    
    artist_schema = StructType([ \
        StructField("artist_id",StringType()), \
        StructField("name",StringType()), \
        StructField("location",StringType()), \
        StructField("latitude", DoubleType()), \
        StructField("longitude", DoubleType())
    ])
    artist_data = output_data + "artists"
    artist_df = spark.read.schema(artist_schema).parquet(artist_data)
    artist_df.createOrReplaceTempView("artists")

    # extract columns from joined song, artist, and log datasets to create songplays table 
    songplays_table = spark.sql("""
        select from_unixtime(l.ts / 1000) as start_time
            , l.userId as user_id
            , l.level
            , s.song_id
            , s.artist_id
            , l.sessionId as session_id
            , l.location
            , l.userAgent as user_agent
            , year(from_unixtime(l.ts / 1000)) as year
            , month(from_unixtime(l.ts / 1000)) as month
        from songs s
        join artists a
            on s.artist_id = a.artist_id
        join log_data l
            on l.artist = a.name
            and l.song = s.title
    """)
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode('overwrite').parquet(output_data + "songplays")

def main():
    spark = create_spark_session()
#     input_data = "s3a://udacity-dend/" # use this use S3 input data
    input_data = "data/" # use this for local small dataset as input (for testing only)
    
#     output_data = "s3a://mmoyer-sparkify/"  # use this to publish output to S3
    output_data = "data/output/" # use this to publish output locally (for testing only)
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
