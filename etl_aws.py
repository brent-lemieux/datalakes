"""Execute by running:

aws emr add-steps --cluster-id j-1QBDBZ27YPE4W --steps Type=spark,Name=mySparkApp,Args=[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=false,s3://bwl-spark-example/etl_aws.py],ActionOnFailure=CONTINUE
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark_session():
    """Create spark session.

    Returns:
        spark (SparkSession) - spark session connected to AWS EMR cluster
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Process the song data into parquet files.

    Arguments:
        spark (SparkSession) - spark session connected to AWS EMR cluster
        input_data (str) - AWS S3 bucket of source data
        output_data (str) - AWS S3 bucket for writing processed data
    """
    song_data = input_data + 'song_data/*/*/*/*.json'
    df = spark.read.json(song_data)
    # Extract relevant columns and write songs and artists tables.
    songs_table = df.select(
        'song_id', 'title', 'artist_id', 'artist_name', 'year', 'duration'
    )
    songs_table.dropDuplicates() \
        .write \
        .partitionBy('year', 'artist_id') \
        .save(output_data + 'songs')
    artists_table = df.selectExpr(
        'artist_id', 'artist_name as name', 'artist_location as location',
        'artist_latitude as latitude', 'artist_longitude as longitude'
    )
    artists_table.dropDuplicates() \
        .write \
        .save(output_data + 'artists')


def process_log_data(spark, input_data, output_data):
    """Process the log data into parquet files.

    Arguments:
        spark (SparkSession) - spark session connected to AWS EMR cluster
        input_data (str) - AWS S3 bucket of source data
        output_data (str) - AWS S3 bucket for writing processed data
    """
    log_data = input_data + 'log_data/*/*/*.json'
    df = spark.read.json(log_data)
    df = df.filter(df['page'] == 'NextSong')
    # Make transformations and extract columns to write user and time tables.
    users_table = df.selectExpr(
        'userId as user_id', 'firstName as first_name',
        'lastName as last_name', 'gender', 'level'
    )
    users_table.dropDuplicates() \
        .write \
        .save(output_data + 'users')
    is_weekday = F.udf(lambda x: 1 if int(x) < 6 else 0)
    time_table = df.selectExpr('cast(ts/1000 as timestamp) as start_time')
    time_table = time_table.withColumn('hour', F.hour('start_time')) \
        .withColumn('day', F.dayofmonth('start_time')) \
        .withColumn('week', F.weekofyear('start_time')) \
        .withColumn('month', F.month('start_time')) \
        .withColumn('year', F.year('start_time')) \
        .withColumn('day_of_week', F.date_format('start_time', 'u'))
    time_table = time_table.withColumn('weekday', is_weekday('day_of_week'))
    time_table.dropDuplicates() \
        .write \
        .partitionBy('year', 'month') \
        .save(output_data + 'time')
    # Create songplays table from log data and song data joined and write.
    song_df = spark.read.parquet(output_data + 'songs/*/*/*.parquet')
    songplays_table = df.join(
        song_df, df.song == song_df.title & df.artist == song_df.artist_name,
        how='left'
    )
    songplays_table = songplays_table \
        .withColumn('songplay_id', monotonicallyIncreasingId())
    songplays_table = songplays_table.selectExpr(
        'songplay_id', 'cast(ts/1000 as timestamp) as start_time',
        'userId as user_id', 'level', 'song_id', 'artist_id',
        'sessionId as session_id', 'location', 'userAgent as user_agent'
    )
    songplays_table = songplays_table \
        .withColumn('month', F.month('start_time')) \
        .withColumn('year', F.year('start_time'))
    songplays_table.dropDuplicates() \
        .write \
        .partitionBy('year', 'month') \
        .save(output_data + 'songplays')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://bwl-spark-example/datalakes-project/"
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
