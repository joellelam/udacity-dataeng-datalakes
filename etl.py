#!/opt/conda/bin/python

import configparser
import os
import datetime
import sys, getopt

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import weekofyear, monotonically_increasing_id, udf, col, from_unixtime, year, month, dayofmonth, hour, weekofyear, date_format, to_timestamp, to_date, from_unixtime
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['default']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['default']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create Spark Session"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    sc=spark.sparkContext
    hadoop_conf=sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    #TODO: Pull from environment
    hadoop_conf.set("fs.s3n.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID'])
    hadoop_conf.set("fs.s3n.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY'])
    #hadoop_conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    return spark

def process_song_data(spark, input_data, output_data):
    """
    Process song data.
    
    Keyword arguments:
    spark -- the spark session
    input_data -- the input data source, this should be a directory locally or on s3.
    output_data -- the output data target, this should be a directory locally or on s3
    """


    #songs - songs in music database
    #song_id, title, artist_id, year, duration
    
    # get filepath to song data file
    song_data = input_data + "/song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    
    # extract columns to create songs table
    songs_table = df.select('song_id','title', 'artist_id', 'year', 'duration')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(['year', 'artist_id']).parquet(output_data + "/songs.parquet", mode="overwrite")    
    
    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude' )
    artists_table = artists_table.withColumnRenamed("artist_name", "name")
    artists_table = artists_table.withColumnRenamed("artist_location", "location")
    artists_table = artists_table.withColumnRenamed("artist_latitude", "latitude")
    artists_table = artists_table.withColumnRenamed("artist_longitude", "longitude")
    artists_table = artists_table.dropDuplicates(["artist_id"])
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "/artists.parquet", mode="overwrite")

def process_log_data(spark, input_data, output_data):
    """
    Process log data.
    
    Keyword arguments:
    spark -- the spark session
    input_data -- the input data source, this should be a directory locally or on s3.
    output_data -- the output data target, this should be a directory locally or on s3
    """

    # get filepath to log data file
    log_data = input_data + "/log_data/*.json"
    
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    # TODO: Decide if I need this?
    # df = 

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'level')
    users_table = users_table.withColumnRenamed("userId", "user_id")
    users_table = users_table.withColumnRenamed("firstName", "first_name")
    users_table = users_table.withColumnRenamed("lastName", "last_name")
    users_table = users_table.dropDuplicates().filter("user_id != ''").sort(["user_id"])
    
    # write users table to parquet files
    # TODO: Enable this again
    users_table.write.parquet("s3n://udacityproject4/users.parquet", mode="overwrite")

    # time - timestamps of records in songplays broken down into specific units
    # start_time, hour, day, week, month, year, weekday

    #Resolve Start Time
    time_table = df.select('ts').withColumn("start_time", to_timestamp(from_unixtime(F.col('ts')/1000)))

    #Resolve Hour
    get_hour = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0).hour)
    time_table = time_table.withColumn("hour", get_hour(df.ts))

    #Resolve Day
    get_day = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0).day)
    time_table = time_table.withColumn("day", get_day(df.ts))

    #Resolve Month
    get_month = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0).month)
    time_table = time_table.withColumn("month", get_month(df.ts))

    #Resolve Year
    get_year = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0).year)
    time_table = time_table.withColumn("year", get_year(df.ts))

    # Resolve WeekDay
    get_weekday = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0).weekday())
    time_table = time_table.withColumn("weekday", get_weekday(df.ts))

    # Resolve Week In Year
    time_table = time_table.withColumn("week", weekofyear(time_table.start_time))

    # Remove TS since it's no longer needed
    time_table = time_table.drop('ts')

    # read in song data to use for songplays table
    # get filepath to song data file
    song_data = input_data + "/song_data/*/*/*/*.json"
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    df = df.select('ts', 'userId', 'level', 'artist', 'sessionId', 'userAgent','song').sort('song')
    df = df.withColumn("start_time", to_timestamp(from_unixtime(F.col('ts')/1000)))
    songplays_table = df.join(song_df, song_df.title == df.song)
    songplays_table = songplays_table.withColumnRenamed("userId", "user_id")
    songplays_table = songplays_table.withColumnRenamed("sessionId", "session_id")
    songplays_table = songplays_table.withColumnRenamed("artist_location", "location")
    songplays_table = songplays_table.withColumnRenamed("userAgent", "user_agent")
    songplays_table.select('start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent')
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet( output_data + "/songplays.parquet", mode="overwrite")
    print(songplays_table.toPandas().head(3))


def main():
    """Main method for etl"""
   
    input_data = "s3n://udacity-dend"
    output_data = "s3n://udacityproject4"
    
    # Argument Management Borrowed From: https://www.tutorialspoint.com/python/python_command_line_arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:],"hi:o:",["idata=","odata="])
    except getopt.GetoptError:
        print ('etl.py -i <inputdata> -o <outputdata>')
        sys.exit(2)
        
    print("opts: ", opts)
    for opt, arg in opts:      
        if opt == '-h':
            print ('etl.py -i <inputdata> -o <outputdata>')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            input_data = arg
        elif opt in ("-o", "--ofile"):
            output_data = arg
    print ('Input data folder is "', input_data)
    print ('Output data folder is "', output_data)
    
    spark = create_spark_session()
    
    process_song_data(spark, input_data, output_data)    
    #process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
