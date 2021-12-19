import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This func to create spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This procedure processes a song file
    It extracts the data from S3 bucket using Spark 
    in order to process the data then store the songs and artists table's into other S3 bucket.
    """
    # get filepath to song data file
    song_data = f'{input_data}/song_data/A/A/*/*.json'
  
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id","title","artist_id","year","duration"]).dropDuplicates()
  
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode('overwrite').parquet("s3a://datalakedendudacity1/songs_table")

    # extract columns to create artists table
    artists_table = df.select(["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet("s3a://datalakedendudacity1/artists_table.parquet")

    print("************************************* process_song_data SUCSSES *************************************")
def process_log_data(spark, input_data, output_data):
    """
    This procedure processes a log file
    It extracts the data from S3 bucket using Spark 
    in order to process the data with spark then store the users, time 
    and songplay table's into other S3 bucket.
    """
    # get filepath to log data file
    log_data = f'{input_data}/log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(["userId","firstName","lastName","gender","level"]).dropDuplicates() 
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet("s3a://datalakedendudacity1/users_table")

    # create timestamp column from ts column
    df = df.withColumn("start_time", F.to_timestamp(df.ts/1000))
        
    # extract columns to create time table
    time_table = df.select("start_time")\
            .withColumn("hour", F.hour("start_time"))\
            .withColumn("day", F.dayofweek("start_time"))\
            .withColumn("week", F.weekofyear("start_time"))\
            .withColumn("month", F.month("start_time"))\
            .withColumn("year", F.year("start_time"))\
            .dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode('overwrite').parquet("s3a://datalakedendudacity1/time_table")

    # read in song data to use for songplays table
    song_df = spark.read.json(f'{input_data}/song_data/A/A/*/*.json')
    
    song_df.createOrReplaceTempView("songs_table")
    df.createOrReplaceTempView("log_table")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
          SELECT SE.ts, 
            SE.userId,
            SE.level, 
            SS.song_id,                      
            SS.artist_id, 
            SE.sessionId,
            SS.artist_location,
            SE.userAgent
          FROM songs_table SS , log_table SE
          WHERE SE.song = SS.title
                AND SE.artist = SS.artist_name
          """)
    # extract year and month from ts in songplays_table 
    songplays_table = songplays_table.withColumn("start_time", F.to_timestamp(songplays_table.ts/1000))
    songplays_table = songplays_table.select(['start_time','ts','userId','level','song_id','artist_id','sessionId','artist_location','userAgent'])\
                        .withColumn("month", F.month("start_time"))\
                        .withColumn("year", F.year("start_time"))
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode('overwrite').parquet("s3a://datalakedendudacity1/songplays_table")
    print("************************************* process_log_data SUCSSES *************************************")
    
def main():
    spark = create_spark_session()
    input_data = 's3a://udacity-dend/'
    output_data = 's3a://datalakedendudacity1/'
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
