import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """ Create or retrieve a Spark session
    
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ Extract songs metadata from S3, processes it using Spark and loads it back into S3 as a set of dimensional tables in parquet format
    @spark: spark session defined previously
    @input_data: S3 bucket where the input data is located
    @output_data: S3 bucket where to load the processed output data
    
    """
    # get filepath to song data file
    song_data = input_data + "song-data/*/*/*/*.json"
    
    # read song data file
    ## define schema for songs JSONs
    song_schema = StructType([
        StructField("song_id", StringType(), True),
        StructField("year", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("artist_id", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_latitude", DoubleType(), True),
        StructField("artist_longitude", DoubleType(), True),
    ])
    
    print('Loading songs metadata from S3')
    df = spark.read.json(song_data, schema=song_schema)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'artist_id', 'year', 'duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet((output_data + "songs"), 'overwrite')
    print("songs table created in parquet")

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude','artist_longitude').dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet((output_data + "artists"), 'overwrite')
    print("artists table created in parquet")


def process_log_data(spark, input_data, output_data):
    """ Extract logs data from S3, processes it using Spark and loads it back into S3 as a set of fact/dimensional tables in parquet format
    @spark: spark session defined previously
    @input_data: S3 bucket where the input data is located
    @output_data: S3 bucket where to load the processed output data
    
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    print('Loading logs data from S3')
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(col("page") == 'NextSong')

    # extract columns for users table    
    users_table = df.select(
        col("userId").alias("user_id"),
        col("firstName").alias("first_name"),
        col("lastName").alias("last_name"),
        "gender","level").dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet((output_data + "users"), 'overwrite')
    print("users table created in parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn("timestamp", get_timestamp('ts')) 
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select(
        col('datetime').alias('start_time'),
        hour('datetime').alias('hour'),
        dayofmonth('datetime').alias('day'),
        weekofyear('datetime').alias('week'),
        month('datetime').alias('month'),
        year('datetime').alias('year')
    )
    
    time_table = time_table.dropDuplicates(['start_time'])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet((output_data + 'time'), 'overwrite')
    print("time table created in parquet")

    # read in song data to use for songplays table
    song_data = input_data + "song-data/*/*/*/*.json"
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df.join(df, song_df.artist_name == df.artist)
    songplays_table = songplays_table.select(
            col('ts').alias('ts'),
            col('userId').alias('user_id'),
            col('level').alias('level'),
            col('song_id').alias('song_id'),
            col('artist_id').alias('artist_id'),
            col('sessionId').alias('session_id'),
            col('location').alias('location'),
            col('userAgent').alias('user_agent'),
            col('year').alias('year'),
            month('datetime').alias('month')
        )
    
    songplays_table = songplays_table.\
        withColumn("start_time", get_datetime(songplays_table.ts)).\
        withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").parquet((output_data+"songplays"), 'overwrite')
    print("songplays table created in parquet")


def main():
    """Main function that executes process_song_data() and process_log_data() on songs and logs files  
    
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-project4/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
