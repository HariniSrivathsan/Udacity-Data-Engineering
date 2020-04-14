

Introduction

A startup company called Sparkify want to analyze the data they have been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to.

This can be easily analyzed by creating a star schema in Postgres Database and establising ETL pipelines to optimize queries for song play analysis.

Project Description

In this project, I have to model data with Postgres and build and ETL pipeline using Python. On the database side, I have to define dimension and fact tables for a Star Schema for a analytic focus.

I should also create ETL pipelines would transfer data from jason files located in two local directories into these tables in Postgres using Python and SQL.

Schema for Song Play Analysis

Dimension Tables

users in the app - user_id, first_name, last_name, gender, level

songs in music database -song_id, title, artist_id, year, duration

artists in music database - artist_id, name, location, latitude, longitude

Time - timestamps of records in songplays broken down into specific units - start_time, hour, day, week, month, year, weekday.

Fact Table

songplays records in log data associated with song plays


Project Design

Database Design is denormalized/optimized with fewer tables. It requires only lesser number of joins to slice and dice the data.

ETL Design is to read data from various json files and parse/format accordingly to store it in appropriate tables.


Steps

Step 1: Creating "sql_queries.py" in python using pycharm terminal. It has sql queries for dropping & recreating tables, for ingesting data into the tables.
Step 2: Creating & running "create_tables.py" in python using pycharm terminal. It creates database in Postgres & calls the drop & create table query lists from sql_queries.py 
Step 3: Creating & running "etl.py" in python using pycharm terminal. It  has code to format & ingest data into appropriate tables from jason source files.       

Jupyter Notebook files:

etl.ipynb, a Jupyter notebook file is for verifying each etl commands on a subset of data for unit testing.

test.ipynb displays the first few rows of each table and helps us check your data & its format in each table after processing.

