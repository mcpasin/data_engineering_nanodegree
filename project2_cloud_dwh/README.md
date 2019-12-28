# Project 3: Data Warehouse in Amazon Redshift


* [Intro](#Intro)
* [Files structure](#Files-structure)
* [How to run](#How-to-run)
* [DB schema design](#DB-schema-design)
* [Analytics queries examples](#Analytics-queries-examples)

--------------------------------------------



### Intro
A startup called **Sparkify** wants to analyze the data they've been collecting on _songs_ and _user activity_ on their new **music streaming app**. The analytics team is particularly interested in understanding what songs users are listening to. Currently, **their data resides in Amazon S3**, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The goal of the project is to **move their processes and data onto the cloud**. This includes building an ETL pipeline that:

1) extracts their data from S3
2) stages them in Redshift, and 
3) transforms data into **a set of dimensional tables in Redshift** for their analytics team to continue finding insights in what songs their users are listening to. 



### Files structure

Data is pre-loaded in a public S3 bucket on the us-west-2 region as follows:
* Song data: `s3://udacity-dend/song_data`
* Log data: `s3://udacity-dend/log_data`


The project includes the following files:
* `sql_queries.py`: includes all sql queries needed for creating tables and inserting data in it
* `create_table.py`: includes functions needed to create staging and final analytical tables on Redshift
* `etl.py`: includes functions to load data from S3 into staging tables in Redshift and then insert that data into analytical tables on Redshift
* `dwh.cfg`: stores info needed to create Redshift cluster




### How to run
To run this project you need to follow these steps:

1. Launch a Redshift cluster and create an IAM role that has read access to S3 (for performance reason it's recommended creating the cluster in the same region as the S3 bucket below)
2. Add Redshift database and IAM role into to `dwh.cfg`
3. Run `python create_tables.py` to initialize required tables in Redshift
4. Run `python etl.py` to load data into the tables
5. Check the table schemas in your Redshift database. You can run queries either using Query Editor in the AWS Redshift console or any other client, etc.

*Note:* We recommend to delete your redshift cluster when finished to avoid incurring in unwanted costs.




### DB schema design

**Staging tables**
* staging_events
* staging_songs

To built final analytical tables a Star Schema has been defined as follows:

**Fact table**
* songplays

**Dimension tables**
* users
* songs
* artists
* time




### Analytics queries examples

###### How many distinct users have played a song?

```SQL
SELECT 
count(distinct user_id) 
FROM songplays;
```


###### What is the busiest time of the day in the app?
```SQL
WITH tmp as (
    SELECT 
    s.songplay_id, 
    t.hour 
    FROM songplays s 
    LEFT JOIN time t 
    ON s.start_time = t.start_time
) 
SELECT 
hour, 
count(distinct songplay_id) as count_plays 
FROM tmp 
GROUP BY hour 
ORDER BY count_plays desc 
LIMIT 10;
```