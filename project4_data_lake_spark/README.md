# Project 4: Data Lake


* [Intro](#Intro)
* [Files structure](#Files-structure)
* [How to run](#How-to-run)
* [DB schema design](#DB-schema-design)

--------------------------------------------



### Intro
A startup called **Sparkify** wants to analyze the data they've been collecting on _songs_ and _user activity_ on their new **music streaming app**. The analytics team is particularly interested in understanding what songs users are listening to. Currently, **their data resides in Amazon S3**, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The goal of the project is to **create a Data Lake hosted in Amazon S3**. This includes building an ETL pipeline that:

1) extracts their data from S3
2) processes it using Spark running on a AWS cluster
3) loads the data back into S3 as a set of dimensional tables in Parquet format for their analytics team to continue finding insights in what songs their users are listening to. 



### Files structure

Data is pre-loaded in a public S3 bucket on the us-west-2 region as follows:
* Song data: `s3://udacity-dend/song_data`
* Log data: `s3://udacity-dend/log_data`


The project includes the following files:
* `etl.py`: includes functions to load data from S3, process via Spark and lods it back into S3 as parquet.
* `dl.cfg`: stores AWS access key and secret key to access the EMR cluster




### How to run
To run this project you need to follow these steps:

1. Create a cluster in Amazon EMR. Make sure to include Spark among other relevant applications. 
2. Create a S3 bucket named `udacity-dend-project4` (or use your own name) where you will load your processed tables in parquet format.
3. Include your AWS keys in `dl.cfg` file
4. Run `python etl.py` to to execute the entire ETL process.

Alternatively, you can also run the whole process inside Amazon EMR via a Jupyter notebook. Since you're already inside your cluster, Spark is already available and you don't need to authenticate and crate a session. You just to enter the magic cell `%%spark` and you will be able to run Spark commands (make sure to load required libraries).

*Note:* We recommend to delete your Amazon EMR cluster when finished to avoid incurring in unwanted costs.




### DB schema design

To build the final analytical tables a Star Schema has been defined as follows:

**Fact table**
* songplays

**Dimension tables**
* users
* songs
* artists
* time