import configparser
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, hour
from pyspark.sql.types import IntegerType, StringType

SONG_DATA = "song_data/*/*/*/*.json"
LOG_DATA = "log_data/*/*/*.json"

config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        It retrieves a Spark session using Apache Hadoop packages
    :return: A Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.2") \
        .getOrCreate()
    return spark


def process_data(spark, input_data, output_data):
    """
        It executes the complete ETL process for this project.
        Reads from data source, create songs, artist, time, user and song_plays
        data frames and write them into a S3 bucket
    :param spark: A Spark session
    :param input_data: Directory where data will be read
    :param output_data: Directory where data will be written when the process finish
    :return: NA
    """
    # get filepath to song data file
    song_data_read_path = os.path.join(input_data, "%s" % SONG_DATA)
    # get filepath to log data file
    log_data_read_path = os.path.join(input_data, "%s" % LOG_DATA)

    # read song data file
    raw_song_df = spark.read.json(song_data_read_path)

    # process artists and songs data frames
    process_artist_df(output_data, raw_song_df)
    songs_df = process_song_df(output_data, raw_song_df)

    # read events log file
    raw_log_df = spark.read.json(log_data_read_path)

    # filter it by NextSong event
    next_song_df = raw_log_df \
        .filter(raw_log_df.page == 'NextSong')

    # process users, time and song_plays data frames
    process_users_df(output_data, next_song_df)
    process_time_df(output_data, next_song_df)
    process_song_plays_df(output_data, next_song_df, songs_df)


def process_artist_df(output_data, raw_df):
    """
        It creates an artists data frame and write it in
        a S3 bucket.
    :param output_data: Base path directory where artists data frame will be saved
    :param raw_df: Raw song data frame used to extract artist data
    :return:
    """
    # extract columns to create artists table
    artists_df = raw_df.select(raw_df.artist_id,
                               raw_df.artist_name,
                               raw_df.artist_location,
                               raw_df.artist_latitude,
                               raw_df.artist_longitude) \
        .drop_duplicates(['artist_id'])

    # write artists table to parquet files
    artist_data_save_path = os.path.join(output_data, 'artists')
    artists_df.write.parquet(artist_data_save_path, mode='overwrite')


def process_song_df(output_data, row_df):
    """
        Creates, write into S3 bucket and return a songs data frame
    :param output_data: Directory where data will be written when the process finish
    :param row_df: Raw song data frame used to extract song data
    :return:
    """
    # extract columns to create songs data frame
    songs_df = row_df.select(row_df.song_id,
                             row_df.title,
                             row_df.artist_id,
                             row_df.year,
                             row_df.duration) \
        .drop_duplicates(['song_id'])

    # write songs table to parquet files partitioned by year and artist
    song_data_save_path = os.path.join(output_data, 'songs')
    songs_df.write.partitionBy('year', 'artist_id').parquet(song_data_save_path, mode='overwrite')

    return songs_df


def process_users_df(output_data, next_song_df):
    """
        Creates and writes an users data frame
    :param output_data: Base directory where data will be written when the process finish
    :param next_song_df: Filtered events data frame
    :return: NA
    """
    # extract columns for users table
    users_df = next_song_df \
        .select(
            next_song_df.userId.cast(IntegerType()),
            next_song_df.firstName,
            next_song_df.lastName,
            next_song_df.gender,
            next_song_df.level) \
        .drop_duplicates(['userId'])

    # write users table to parquet files
    users_data_save_path = os.path.join(output_data, 'users')
    users_df.write.parquet(users_data_save_path, mode='overwrite')


def date_from_ts(ts):
    """
        Gets a date object from a timestamp
    :param ts: Timestamp in milliseconds
    :return: A datetime object
    """
    import datetime
    return datetime.datetime.fromtimestamp(ts / 1000)


@udf(IntegerType())
def hour(ts):
    """
        Returns the hour of the day in 24 H format
    :param ts: Timestamp in milliseconds
    :return: Hour of the day. Ex. 23
    """
    return date_from_ts(ts).hour


@udf(IntegerType())
def day(ts):
    """
        Returs the day of a given timestamp
    :param ts: Timestamp in milliseconds
    :return: Day of the year. Ex. 12
    """
    return date_from_ts(ts).day


@udf(IntegerType())
def week(ts):
    """
        Retrieves the week number of a given timestamp
    :param ts: Timestamp in milliseconds
    :return: Week of the year. Ex. 10
    """
    return int(date_from_ts(ts).strftime("%W"))


@udf(StringType())
def month(ts):
    """
        Return the month name of the given timestamp
    :param ts: Timestamp in milliseconds
    :return: Month name of the given timestamp. Ex. March
    """
    return date_from_ts(ts).strftime("%B")


@udf(IntegerType())
def year(ts):
    """
        Return year of the given date.
    :param ts: Timestamp in milliseconds
    :return: Year of the given timestamp. Ex. 2020
    """
    return date_from_ts(ts).year


@udf(StringType())
def weekday(ts):
    """
        Returns week day. Ex. Monday
    :param ts: Timestamp in milliseconds
    :return: The week day of the given timestamp
    """
    return date_from_ts(ts).strftime("%A")


def process_time_df(output_data, next_song_df):
    """
        Creates a time data frame and save it in a S3 bucket
    :param output_data: Base directory where data will be written when the process finish
    :param next_song_df: Filtered raw song data frame
    :return: NA
    """

    ts_df = next_song_df \
        .select(next_song_df.ts.alias("start_time")) \
        .withColumn("hour", hour(col("start_time"))) \
        .withColumn("day", day(col("start_time"))) \
        .withColumn("week", week(col("start_time"))) \
        .withColumn("month", month(col("start_time"))) \
        .withColumn("year", year(col("start_time"))) \
        .withColumn("weekday", weekday(col("start_time"))) \
        .drop_duplicates(['start_time'])

    # write time table to parquet files partitioned by year and month
    time_data_save_path = os.path.join(output_data, 'time')
    ts_df.write.partitionBy('year', 'month') \
        .parquet(time_data_save_path, mode='overwrite')


def process_song_plays_df(output_data, events, songs_df):
    """
        Creates the fact data frame song_plays and write it
        in a S3 bucket
    :param output_data: Base directory where data will be written when the process finish
    :param events: Raw events filtered by NextSong event
    :param songs_df: Filtered and clean songs data frame
    :return: NA
    """

    # extract columns from joined song and log datasets to create songplays table
    song_plays_df = events.join(songs_df,
                                (events.song == songs_df.title) &
                                (events.length == songs_df.duration),
                                'left_outer') \
        .select(events.ts.alias('start_time'),
                events.userId.alias('user_id'),
                events.level,
                songs_df.song_id,
                songs_df.artist_id,
                events.sessionId.alias('session_id'),
                events.location,
                events.userAgent.alias('user_agent')) \
        .withColumn('year', year(col('start_time'))) \
        .withColumn('month', month(col('start_time')))

    # write song plays table to parquet files partitioned by year and month
    song_plays_save_path = os.path.join(output_data, 'song_plays')
    song_plays_df.write.partitionBy('year', 'month').parquet(song_plays_save_path, mode='overwrite')


def main():
    """
        Application starting point
    :return:
    """
    # Get Spark session
    spark = create_spark_session()
    # Log's repository
    input_data = "s3a://udacity-dend/"
    # Processed files destination
    output_data = "s3a://data-lake-jl/"

    process_data(spark, input_data, output_data)

    # Stop spark job
    spark.stop()


if __name__ == "__main__":
    main()
