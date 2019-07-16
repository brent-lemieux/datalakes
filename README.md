# Sparkify Data Lake

### Background
The purpose of this project is to build a data lake that will allow Sparkify's analytics team to continue extracting insights from their large amount of data.  

Due to the data increasing in size, using tools like Apache Spark will radically decrease our ETL processing time. Using a data lake allows for new data from other sources to be integrated more easily -- we can collect all types of data without worrying about potential value.

### Schema
The output of the ETL pipeline is a star schema saved in parquet files. In the star schema, the `songplays` table is the fact table. The dimension tables are `songs`, `artists`, `users`, and `time`.

The `songplays` table is partitioned by year and month. The `songs` table is partitioned by year and artist_id. The `time` table is partitioned by year and month.

This star schema will allow users to quickly perform analysis on song plays. The log and song data is maintained in our data lake so that more advanced users can dive deeper into the data.  Changes can also be made in the future if fields excluded from this schema are deemed to be useful.

### ETL Pipeline
The ETL pipeline extracts data from `s3a://udacity-dend/`, processes it using Apache Spark, and writes the transformed data to `s3://bwl-spark-example/datalakes-project`. Within the output directory, the data is subdivided into sud-directories corresponding to table name: `/songplays/`, `/songs/`, `/artists/`, `/users/`, and `/time/`. The transformations include selecting columns useful for analysts, filtering log data to `page == 'NextSong'`, removing duplicates where applicable, and creating columns that are functions of other columns (i.e., hour from start_date).

### Execution
Run the script by calling `python etl.py`

Alternatively, run:
```
aws emr add-steps --cluster-id <your-cluster-id> --steps Type=spark,Name=mySparkApp,Args=[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=false,s3://bwl-spark-example/etl_aws.py],ActionOnFailure=CONTINUE
```
This requires the AWS CLI to be [installed](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html) and [configured](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).
