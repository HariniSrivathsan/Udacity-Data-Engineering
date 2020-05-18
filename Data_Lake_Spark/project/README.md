Data Lake with Spark: Song Play Analysis


Preamble

Data source is provided by one public S3 buckets. This bucket contains info about songs, artists and actions done by users (which song are listening, etc..). The objects contained in the bucket are JSON files. The entire elaboration is done with Apache Spark
Spark process

The ETL job processes the song files then the log files. The song files are listed and iterated over entering relevant information in the artists and the song folders in parquet. The log files are filtered by the NextSong action. The subsequent dataset is then processed to extract the date , time , year etc. fields and records are then appropriately entered into the time, users and songplays folders in parquet for analysis.
Project structure

This is the project structure contains 

    /data - A folder that cointains two zip files, helpful for data exploration
    etl.py - The ETL engine done with Spark, data normalization and parquet file writing.
    dl.cfg - Configuration file that contains info about AWS credentials
    
 Process:
  Step 1: create an aws EMR cluster.
  Step 2: Run bootsrap.sh to configure the environment
  step 3: Run etl.py

  







