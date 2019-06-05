Purpose:
The purpose of this project is use Spark to create an ETL to load the data from S3 after that make some process on the data and load the parquet files back into S3.

**Description:**
1. Use My account in AWS and add my credentials in dl.cfg file.it is used to access my S3 in AWS to load the data .

2. In order to convert the 'ts' column in the song_table to datetime, 2 User-Defined Functions were created:

>get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000), TimestampType())
>udf(lambda x: F.to_date(x), TimestampType())

3. I use pyspark sql so this required creating temporary tables:

>df.createOrReplaceTempView("songs")
>df.createOrReplaceTempView("logs")

4. To create parquet file i used the below command with overwrite mode if exists .

>artists_table.write.mode('overwrite').parquet(output_data+"/users")

5. To create the songs,artist tables in second function , I decided to read from parquet files that I created for this tables.The below command:

>artist_df=spark.read.parquet(output_data+"/artists")
>song_df.createOrReplaceTempView("artists")

6. There are two functions have the same inputs :

>1-process_song_data--> Process song_input data using spark and output as parquet to S3
>2-process_log_data --> Process log_input_data data using spark and output as parquet to S3


**How to run:**
1. Replace AWS IAM Credentials in dl.cfg
2. Modify input and output data paths in etl.py main function
3. In the terminal, run python etl.py
