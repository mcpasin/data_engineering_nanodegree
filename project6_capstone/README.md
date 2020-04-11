# Capstone Project: Immigration Data for the United States


* [Intro](#Intro)
* [Solution](#End-solution)

--------------------------------------------



### Intro
In this capstone project we are focusing on the **immigration data for the US**. The main idea is to build a broader view of the immigration behaviour to the US by joining it with a series of potentially interesting attributes such as:  

* the travel mode, seasonality, reason of travel, origin/destinations and avg. time spent in the US
* the type of airports used to enter the US
* the city demographics to which they're traveling
* temperature statistics about the location where they are traveling from/to

Given the above objective, **the end solution will be a set of fact and dimensional tables hosted in Amazon S3** that the final user can easily access and analyse to generate new insights.



### Solution
An **ETL data pipeline** is built (see `etl.py`) to perform the following steps:  

1. Extract data from different sources
2. Process them with the help of Python and Spark
3. Load the final tables into S3 in Parquet format

In `Capstone_Project.ipynb` notebook file we include all the steps performed during data exploration, it's usful to reproduce it step by step.

The datasets we used are:  

* I94 immigration data: this dataset comes from the US National Tourism and Trade Office and is available locally.
* Airport Code table: this is a simple table of airport codes and corresponding cities. It comes from here.
* U.S. City Demographic data: this dataset comes from OpenSoft.
* World Temperature Data: this dataset came from Kaggle.


