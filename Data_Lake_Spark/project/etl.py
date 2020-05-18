import configparser
from datetime import datetime
import os
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('SparkCreds','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('SparkCreds', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):

    """ Proving JSON structure to Spark """
    songdata_schema = StructType([
        StructField("song_id", StringType(), True),
        StructField("year", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("artist_id", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_latitude", DoubleType(), True),
        StructField("artist_longitude", DoubleType(), True),
    ])

    
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data, schema=songdata_schema)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'artist_id', 'year', 'duration')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + "songs")

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude',
                              'artist_longitude')
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):

    """ Proving JSON structure to Spark """
    logdata_schema = StructType([
        StructField("artist", StringType(), True),
        StructField("auth", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("itemInSession", LongType(), True),
        StructField("lastName", StringType(), True),
        StructField("length", DoubleType(), True),
        StructField("level", StringType(), True),
        StructField("location", StringType(), True),
        StructField("method", StringType(), True),
        StructField("page", StringType(), True),
        StructField("registration", DoubleType(), True),
        StructField("sessionId", LongType(), True),
        StructField("song", StringType(), True),
        StructField("status", LongType(), True),
        StructField("ts", LongType(), True),
        StructField("userAgent", StringType(), True),
        StructField("userId", StringType(), True),
    ])


    
    # get filepath to log data file
    log_data = input_data + 'log-data'

    # read log data file
    print('Reading log data file from log_data')
    df = spark.read.json(log_data, schema = logdata_schema).drop_duplicates()
    
    # filter by actions for song plays
    df = df.filter(col("page") == 'NextSong').filter(df.userId.isNotNull())
    

    # extract columns for users table    
    users_table = df.select(col("userId").alias("user_id"), col("firstName").alias("first_name"),
                            col("lastName").alias("last_name"), "gender", "level")
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "users")

    # create timestamp column from original timestamp column
    tsFormat = "yyyy-MM-dd HH:MM:ss z"
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    #get_timestamp = udf(lambda x: to_timestamp(date_format((df.ts/1000).cast(dataType=TimestampType()), tsFormat), tsFormat))
    df = df.withColumn("ts", get_timestamp(col('ts')))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(col("ts")))
                       
    
    # extract columns to create time table
    #time_table = time_table.select(col("ts").alias("start_time"), hour(col("ts")).alias("hour"), dayofmonth(col("ts")).alias("day"), weekofyear(col("ts")).alias("week"), month(col("ts")).alias("month"), year(col("ts")).alias("year"))

    time_table = df.select(
         col('ts').alias('start_time'),
         hour('datetime').alias('hour'),
         dayofmonth('datetime').alias('day'),
         weekofyear('datetime').alias('week'),
         month('datetime').alias('month'),
         year('datetime').alias('year'),
         date_format('datetime', 'F').alias('weekday')
     )

                
    
    # write time table to parquet files partitioned by year and month
    print('Writing partitioned time-table to Parquet files....')
    time_table.write.partitionBy("year","month").parquet(output_data+"time")

    # read in song data to use for songplays table
    song_data = input_data+"song_data/*/*/*/*.json"
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = song_df.join(df, song_df.artist_name==df.artist).withColumn("songplay_id", monotonically_increasing_id()).withColumn('start_time', to_timestamp(date_format((col("ts") /1000).cast(dataType=TimestampType()), tsFormat),tsFormat)).select("songplay_id", "start_time", col("userId").alias("user_id"), "level", "song_id", "artist_id", col("sessionId").alias("session_id"), col("artist_location").alias("location"), "userAgent", month(col("start_time")).alias("month"), year(col("start_time")).alias("year"))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").parquet(output_data+"songplays")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
