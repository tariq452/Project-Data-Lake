import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_date, monotonically_increasing_id
from pyspark.sql.types import TimestampType
import pyspark.sql.functions as F

config = configparser.ConfigParser()
config.read('dl.cfg')
#AWS credential
os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']

#Return spark session
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

"""
Input: spark session, song_input_data S3 path, and output_data S3 path
Purpose: Process song_input data using spark and output as parquet to S3
create two files Songs and Artists dimension tables.
Return none
"""
def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data+"song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data) 

    # extract columns to create songs table
    df.createOrReplaceTempView("songs")
    songs_table = spark.sql("""
    SELECT distinct song_id, title, artist_id, year, duration
    FROM songs
    order by song_id
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year","artist_id").parquet(output_data+"/songs")

    # extract columns to create artists table
    artists_table =  spark.sql("""
                   SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                   FROM songs
                   order by artist_id
                   """)
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+"/artists")


""""
#Input: spark session, log_input_data S3 path, and output_data S3 path
#Purpose: Process log_input_data data using spark and output as parquet to S3
create three files 
(users and times) dimension tables
(Songplay) Fact table
Return none
"""
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =input_data+"log_data/*/*/*.json"

    # read log data file
    df =  spark.read.json(log_data)
    
    #filter by actions
    df=df.filter(df.page=='NextSong')
    
    # extract columns for users table    
    df.createOrReplaceTempView("users")
    artists_table =  spark.sql("""
                   SELECT distinct userId , firstName, lastName, gender, level
                   FROM users
                   where userId!=''
                   order by userId 
                   """)
    
    # write users table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+"/users")

    # create timestamp column from original timestamp column
    get_timestamp =udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts).cast("Timestamp")) 
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x:  datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn("start_time", get_datetime(df.ts).cast("date"))
    
    df.createOrReplaceTempView("logs")
    # extract columns to create time table
    time_table =  spark.sql("""
                   SELECT distinct  start_time, hour(start_time) hour, dayofmonth(start_time)day, weekofyear(start_time) week,
                   month(start_time) month  , year(start_time) year, dayofmonth(start_time) weekday
                   FROM logs
                   """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year","month").parquet(output_data+"/times")

    # read in song data to use for songplays table
    
    #read song parquet to get song_id
    song_df = spark.read.parquet(output_data+"/songs")
    song_df.createOrReplaceTempView("songs")
    
    #read artist parquet to join with log file by artist_name
    artist_df=spark.read.parquet(output_data+"/artists")
    artist_df.createOrReplaceTempView("artists")
    
    # create temp view for time
    time_table.createOrReplaceTempView("time")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql(
        """SELECT DISTINCT logs.start_time, logs.userId, logs.level,songs.song_id,
        songs.artist_id,logs.sessionId,logs.location,logs.userAgent ,time.year,time.month
        FROM logs 
        INNER JOIN artists
        ON artists.artist_name = logs.artist 
        INNER JOIN  songs 
        ON songs.artist_id = artists.artist_id
        INNER JOIN time
        ON time.start_time = logs.start_time
        """) 
    
    songplays_table.withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year","month").parquet(output_data+"/songplays")


def main():
    #Create spark session
    spark = create_spark_session()
    
    #Specify data
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://data-engineer-udacity-tt"
   
    
    #Call process functions
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
