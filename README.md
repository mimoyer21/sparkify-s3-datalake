# Readme.md for Sparkify datalake implemented via AWS S3

## Purpose:
The purpose of this datalake is to provide datasets that enable answering questions about user behavior in the fictional Sparkify music streaming app (especially user song 
listening behavior). To do so we ingest the app logs of user behavior and song metadata from S3, process the raw data via Spark, and upload 
processed data back to S3. Analysis to answer business questions can be done on the processed data stored in S3.

## The files in the repository:
* etl.py: This is the core ETL process that we run to load raw data from S3, process data via Spark, and upload processed data back to S3. By design, running it __overwrites__ any existing data in the output paths
* dl.cfg: this file is not included in the repo, but is needed to run this ETL _if and only if_ you are not running this script from within AWS (e.g. if you are running this script locally, you need dl.cfg). Create a 'dl.cfg' file with your own AWS credentials to be able to run this ETL flow (AWS credentials must have access to the source data in S3)
* data/log-data.zip and data/song-data.zip: these are smaller subsets of the S3 input data that can be used for easier testing of the script before running it on the full (larger) S3 input data
* Sparkify.ipynb: this is a Jupyter Notebook version of the ETL flow that can be easily run from within AWS EMR Notebooks. Running from within AWS (in an account with access to the source data) does not require explicitly setting AWS credentials in the script, so you'll notice that those are not included in this ipynb script 

## Datalake schema design:
We've designed the datalake to store user song listening activity under 'songplays' and data on a handful of other relevant entities as listed below. 'Songplays' data is relatively normalized, so users will need to join to other datalake sources to pull in most song/artist/user details.

Datasets:
* songplays: stores the following for each user listening activity: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent, year, month
* songs: user_id, first_name, last_name, gender, level
* artists: artist_id, name, location, latitude, longitude
* users: user_id, first_name, last_name, gender, level
* time: ts, start_time, hour, day, week, month, year, weekday

We've also partitioned some of the tables for improved performance.
Partitioning:
* songplays: year, month
* songs: year, artist_id
* time: year, month

## Example queries and results for song play analysis:
(to be filled in with more examples later if desired)

Example querying data via pyspark to get total number of song plays by all users during a given time period (in this case, 2018):

- create spark session
- read in datasets
- query. multiple options to query, but one is to register temp views to query via `spark.sql`

```python
spark = SparkSession.builder.<...>.getOrCreate()
songplays_df = spark.read.parquet("s3a://mmoyer-sparkify/songplays")
songplays_df.createOrReplaceTempView("songplays")
spark.sql("""
    select year
        , count(*) as songplay_count
    from songplays
    group by year;
""").show()
``` 
