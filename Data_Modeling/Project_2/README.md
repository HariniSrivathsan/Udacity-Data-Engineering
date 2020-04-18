

Project: Data Modeling with Cassandra

Introduction:

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music 

streaming app. There is no easy way to query the data to generate the results, since the data resides in a directory of CSV 

files on user activity on the app. My role is to create an Apache Cassandra database  & tables to answer specific queries on song play data.

Project Overview:

In this project, I have data modeled tables with Apache Cassandra and have completed an ETL pipeline using Python. 

I was provided with part of an ETL pipeline that transfers data from a set of CSV files within a directory to create a streamlined CSV file to model and insert data into Apache Cassandra tables.

Datasets:

Dataset: event_data. The directory of CSV files partitioned by date. Here are examples of filepaths to two files in the dataset: event_data/2018-11-08-events.csv event_data/2018-11-09-events.csv

Project Template:

The project template includes one Jupyter Notebook file, which  will process the event_datafile_new.csv dataset to create a denormalized dataset. I modeled the data tables keeping in mind the queries that needs to run. 

I was given 3 queries and i modeled the data tables accordingly in Apache Cassandra.

Project Steps:

Below are steps that i followed to complete each component of this project.

Modelling NoSQL Database or Apache Cassandra Database:

    Designed tables to answer the queries outlined in the project template
    Wrote Apache Cassandra CREATE KEYSPACE and SET KEYSPACE statements
    Developed  CREATE statement for each of the tables to address each question
    Loaded the data with INSERT statement for each of the tables
    Included IF NOT EXISTS clauses in your CREATE statements to create tables only if the tables do not already exist. We recommend you also include DROP TABLE statement for each table, this way you can run drop and create tables whenever you want to reset your database and test your ETL pipeline
    Tested the data by running the proper select statements with the correct WHERE clause

Build ETL Pipeline:

    Implemented the logic in section Part I of the notebook template to iterate through each event file in event_data to process and create a new CSV file in Python
    Made necessary edits to Part II of the notebook template to include Apache Cassandra CREATE and INSERT three statements to load processed records into relevant tables in your data model
    Tested data by running three SELECT statements after running the queries on your database
    Finally, drop the tables and shutdown the cluster

Files:


Project_1B.ipynb: This is the final version of the file submitted. It has all the queries for importing the data & for generating a new csv file. 

All the results dataset was verified whether all tables had been loaded accordingly as per requirement

Event_datafile_new.csv: This is the final version of combined source data file.It has all the files which are in the folder event_data

Event_Data Folder: Each event file is present separately, so all the files would be combined into one into event_datafile_new.csv
