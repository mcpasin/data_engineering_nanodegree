# Project 1: Data Modeling with Apache Cassandra


* [Intro](#Intro)
* [How to run](#How-to-run)
* [Files structure](#Files-structure)
* [CQL Query examples](#CQL-Query-examples)

--------------------------------------------



### Intro
A startup called **Sparkify** wants to analyze the data they've been collecting on _songs_ and _user activity_ on their new **music streaming app**. The analytics team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

The goal of the project is to **create an Apache Cassandra database** whith tables aimed at answering some specific queries on song play data. This includes setting up the correspondent ETL pipeline to transfer and process raw data located into a local directories to these tables in _Cassandra_ via _Python_ and _SQL_.



### How to run
Python and a Apache Cassandra istance are required to run the project.

Run Jupyter notebook `Project_1B_Project.ipynb` which include the whole ETL pipeline, included the create/insert/drop statements for Cassandra tables.



### Files structure

* /event_data - folder with csv files with events data. One file per day is stored.
* event_datafile_new.csv an intermediate file generated during the ETL process to create a denormalized dataset.
* README.md provides info for the project.



### CQL Query examples


##### Query table 1:  Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4

Below is our create statement. Note that as primary key we use session_id and item_in_session, which combined will make every row unique.
```CQL
create_table1 = """CREATE TABLE IF NOT EXISTS song_by_session_item(
    session_id int, 
    item_in_session int, 
    artist text, 
    song_title text,
    song_length float, 
    PRIMARY KEY (session_id, item_in_session))
"""
```


You can then query the data as follows:
```CQL
select_table1 = """ SELECT 
    artist, 
    song_title, 
    song_length 
    FROM song_by_session_item
    WHERE session_id = 338 
    AND item_in_session = 4
"""
```
