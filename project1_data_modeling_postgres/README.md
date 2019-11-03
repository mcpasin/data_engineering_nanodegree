# Project 1: Data Modeling with Postgres
<br>

* [Intro](#Intro)
* [Data Modeling](#Data-Modeling )
* [How to run](#How-to-run)
* [Files structure](#Files-structure)
* [Query examples](#Query-examples)

--------------------------------------------

<br>
<br>

### Intro
A startup called **Sparkify** wants to analyze the data they've been collecting on _songs_ and _user activity_ on their new **music streaming app**. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The goal of the project is to **create a Postgres database schema with tables optimised for song play analysis**. This includes setting up the correspondent ETL pipeline to transfer and process raw data located into two local directories to these tables in _Postgres_ via _Python_ and _SQL_.

<br/>

### Data Modeling
A **star schema** has been defined with tables as follows:

<br>
#### Fact Table


***songplays***:records in log data associated with song plays i.e. records with page NextSong
- _songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent_

<br>
#### Dimension Tables


***users***: users in the app
- _user_id, first_name, last_name, gender, level_

***songs***: songs in music database
- _song_id, title, artist_id, year, duration_

***artists***: artists in music database
- _artist_id, name, location, latitude, longitude_

***time***: timestamps of records in songplays broken down into specific units
- _start_time, hour, day, week, month, year, weekday_

<br>

### How to run
Python and a Postgres istance are required to run the project.

1. Run `python create_tables.py` to create databases and tables (via terminal for example).
2. Run `etl.py` to populate tables. This script run the whole ETL pipeline: extract data from "./data/song_data" and "./data/log_data" folders, processes it, and insert it into the tables.
3. You can run code chunks in `test.ipynb` Jupyter notebook to check if tables have been populated.

<br>

### Files structure

* /data - folder with JSON files
    * /log_data - subfolder with log files.
    * /song_data - subfolder with song files.
* test.ipynb displays the first few rows of each table to let you check your database.
* create_tables.py drops and creates your tables. Make sure to run this file to reset your tables before each time you run your ETL scripts.
* etl.ipynb reads and processes a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.
* etl.py reads and processes files from song_data and log_data and loads them into tables. It includes ETL functions based on exploration code developed in etl.ipynb.
* sql_queries.py contains all sql queries, and is imported into the last three files above.
* README.md provides info for the project.


<br>

### Query examples
<br>

##### How many distinct users have played a song?
```SQL
SELECT count(distinct user_id) FROM songplays;
```
<br>

##### Break down of users by gender and level of subscription
```SQL
WITH TMP as (SELECT s.user_id, u.gender, s.level FROM songplays s LEFT JOIN users u ON s.user_id=u.user_id) SELECT gender, level, count(distinct user_id) FROM tmp GROUP BY gender, level;
```
<br>

##### What is the busiest time of the day in the app?
```SQL
WITH tmp as (SELECT s.songplay_id, t.hour FROM songplays s LEFT JOIN time t ON s.start_time = t.start_time) SELECT hour, count(distinct songplay_id) as count_plays FROM tmp GROUP BY hour ORDER BY count_plays desc LIMIT 10;
```
<br>







