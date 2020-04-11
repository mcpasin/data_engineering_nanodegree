import configparser
import pandas as pd
from datetime import datetime
import datetime as dt
import numpy as np

## Spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, date_add, col, split, year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld
from pyspark.sql.types import StringType as Str, DoubleType as Dbl, IntegerType as Int, DateType as Date, LongType as Long
from pyspark.sql.functions import desc
from pyspark.sql.functions import asc
from pyspark.sql.functions import sum as Fsum


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create or retrieve a Spark session
    """
    
    spark = SparkSession.builder.\
        config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11").\
        enableHiveSupport().\
        getOrCreate()
    return spark



def process_immigration_data(spark, input_data, output_data):
    """Extract immigration data from provided folder in local machine, process it using Spark and 
    loads it into S3 as a set of tables in parquet format:
        - a fact table with immigration data and 
        - a dimension table with correspondent dates available
    @spark: spark session defined previously
    @input_data: location path for the dataset
    @output_data: S3 bucket name where to load processed output data
    """
    
    
    # Read in immigration dataset
    print('Loading immigration data...')
    #df_spark = spark.read.csv('immigration_data_sample.csv', header = 'True') # to use sample dataset
    df_spark = spark.read.format('com.github.saurfang.sas.spark').\
        load(input_data)

    
    # Read in mapping codes datasets
    print('Loading mapping codes...')
    map_mode_codes = spark.read.csv('map_codes/map_mode_codes.csv', header = 'True')
    map_visa_codes = spark.read.csv('map_codes/map_visa_codes.csv', header = 'True')
    map_state_codes = spark.read.csv('map_codes/map_state_codes.csv', header = 'True')

    map_port_codes = spark.read.csv('map_codes/map_port_codes.csv', header = 'True')
    map_country_codes = spark.read.csv('map_codes/map_country_codes.csv', header = 'True')
    
    # Create temporary tables to be able to use SparkSQL
    print('Processing immigration data...')
    #df_spark_sample.createOrReplaceTempView("df_immigration")  # to use sample dataset
    df_spark.createOrReplaceTempView("df_immigration")  # to use full dataset
    map_mode_codes.createOrReplaceTempView("map_mode_codes")
    map_visa_codes.createOrReplaceTempView("map_visa_codes")
    map_state_codes.createOrReplaceTempView("map_state_codes")

    map_port_codes.createOrReplaceTempView("map_port_codes")
    map_country_codes.createOrReplaceTempView("map_country_codes")
    
    # Join immigration data with mapping codes, convert SAS dates and rename fields
    df_immigration_joined = spark.sql("""
        SELECT
            i.i94yr as year,
            i.i94mon as month,
            i.i94cit as citizenship_country,
            i.i94res as residence_country,
            i.i94port as port,
            i.arrdate,
            date_add(to_date('1960-01-01'), i.arrdate) AS arrival_date,
            coalesce(m.mode, 'Not reported') as arrival_mode,
            coalesce(s.code, 'Not reported') as us_state,
            i.depdate,
            date_add(to_date('1960-01-01'), i.depdate) AS departure_date,
            i.i94bir as respondent_age,
            coalesce(v.visa, 'Not reported') as visa,
            i.dtadfile as date_added,
            i.visapost as visa_issued_department,
            i.occup as occupation,
            i.entdepa as arrival_flag,
            i.entdepd as departure_flag,
            i.entdepu as update_flag,
            i.matflag as match_arrival_departure_fag,
            i.biryear as birth_year,
            i.dtaddto as allowed_to_stay_date,
            i.insnum as ins_number,
            i.airline as airline,
            i.admnum as admission_number,
            i.fltno as flight_number,
            i.visatype as visa_type
        from df_immigration i 
        left join map_mode_codes m on i.i94mode = m.code
        left join map_visa_codes v on i.i94visa = v.code
        left join map_state_codes s on i.i94addr = s.code
    """)
    
    # Additional cleaning and prepare final fact table
    df_immigration_joined.createOrReplaceTempView("df_immigration_joined")
    df_immigration_cleaned = spark.sql("""
        select *
        from df_immigration_joined
        where 1=1
            and respondent_age >= 0
            and arrival_date IS NOT NULL 
            and departure_date IS NOT NULL
            and arrival_date <= departure_date
    """)
    
    fact_immigration = df_immigration_cleaned.\
        drop('arrdate', 'depdate')
    
    # Write fact table into S3 in parquet format
    print('Writing immigration fact table in parquet...')
    
    fact_immigration.write.partitionBy("year", "month", "us_state").parquet((output_data+"fact_immigration"),'overwrite')
    print('Fact immigration table created in parquet')
    
    # Generate a dimension time table using the Immigration fact table
    print('Extracting time data...')
    fact_immigration.createOrReplaceTempView("fact_immigration")
    dim_time = spark.sql("""
        SELECT DISTINCT arrival_date AS date
        FROM fact_immigration
        WHERE arrival_date IS NOT NULL
        UNION
        SELECT DISTINCT departure_date AS date
        FROM fact_immigration
        WHERE departure_date IS NOT NULL
    """)

    dim_time.createOrReplaceTempView("dim_time")
    
    
    dim_time = dim_time.select(
        col('date').alias('date'),
        dayofmonth('date').alias('day'),
        weekofyear('date').alias('week'),
        month('date').alias('month'),
        year('date').alias('year')
    )
    
    # Write dim time table into S3 in parquet format
    print('Writing time dimension table in parquet...')
    dim_time.write.parquet((output_data+"dim_time"),'overwrite')
    print('Time dimension table created in parquet')
    
    # Write also mapping tables for port_codes and country_codes in parquet format: useful to join by city/country
    print('Writing mapping tables in parquet...')
    map_port_codes.write.parquet((output_data+"map_port_codes"),'overwrite')
    map_country_codes.write.parquet((output_data+"map_country_codes"),'overwrite')
    map_state_codes.write.parquet((output_data+"map_state_codes"),'overwrite')
    print('Mapping tables created in parquet')


    
def process_airports_data(spark, input_data, output_data):
    """Extract airport data from provided folder in local machine, process it using Spark and 
    loads it into S3 as a dimension table in parquet format:
    @spark: spark session defined previously
    @input_data: location path for the dataset
    @output_data: S3 bucket name where to load processed output data
    """
        
    # Read in airport data
    print('Loading airport data...')
    df_airports = spark.read.csv(input_data, header = 'True')
    
    #Cleaning: keep only US data, extract state and lat/long
    print('Processing airports data...')
    df_airports_us_clean = df_airports.filter("iso_country == 'US'")\
        .withColumn("state", split(col("iso_region"), "-")[1])\
        .withColumn("latitude", split(col("coordinates"), ",")[0].cast(Dbl()))\
        .withColumn("longitude", split(col("coordinates"), ",")[1].cast(Dbl()))
    
    dim_airports = df_airports_us_clean.\
        drop('continent', 'iso_region', 'coordinates')
    
    # Write dim table into S3 in parquet format
    print('Writing airports dimension table in parquet...')
    
    dim_airports.write.partitionBy("state").parquet((output_data+"dim_airports"),'overwrite')
    print('Dim airports table created in parquet')

    
    
def process_cities_data(spark, input_data, output_data):
    """Extract US cities demographics data from provided folder in local machine, process it using Spark and 
    loads it into S3 as a dimension table in parquet format:
    @spark: spark session defined previously
    @input_data: location path for the dataset
    @output_data: S3 bucket name where to load processed output data
    """
        
    # Read in US cities data
    print('Loading cities data...')
    df_demographics = spark.read.csv(input_data, header = 'True', sep = ";")
    #pd.read_csv(input_data, sep=';')
    
    #Cleaning: no cleaning needed
    print('Processing cities data...')
    df_demographics_clean = df_demographics.select(
        col('City').alias('city'),
        col('State').alias('state_name'),
        col('Median Age').alias('median_age'),
        col('Male Population').alias('male_population'),
        col('Female Population').alias('female_population'),
        col('Number of Veterans').alias('number_veterans'),
        col('Foreign-born').alias('foreign_born'),
        col('Average Household Size').alias('avg_household_size'),
        col('State Code').alias('state_code'),
        col('Race').alias('race'),
        col('Count').alias('count'),
    )

    
    dim_cities_demographics = df_demographics_clean
    
    # Write dim table into S3 in parquet format
    print('Writing cities dimension table in parquet...')
    
    dim_cities_demographics.write.partitionBy("state_code").parquet((output_data+"dim_cities_demographics"),'overwrite')
    print('Dim cities table created in parquet')

    
    
def process_temperature_data(spark, input_data, output_data):
    """Extract world temperature data from provided folder in local machine, process it using Spark and 
    loads it into S3 as a dimension table in parquet format:
    @spark: spark session defined previously
    @input_data: location path for the dataset
    @output_data: S3 bucket name where to load processed output data
    """
        
    # Read in US cities data
    print('Loading temperature data...')
    df_temperature = spark.read.csv(input_data, header = 'True')
    df_temperature.createOrReplaceTempView("df_temperature")
    
    #Cleaning: remove missing data, unnecessary fields and dates and compute avg. statistics by date/city/country
    print('Processing temperature data...')
    df_temperature_clean = spark.sql("""
        SELECT 
            dt as date,
            City as city,
            Country as country,
            avg(AverageTemperature) as avg_temperature,
            avg(AverageTemperatureUncertainty) as avg_temperature_uncertainty
            --count(*)
        FROM df_temperature
        WHERE 1=1
            and dt >='1960-01-01'
            --and dt >= (select min(date) from dim_time) --make sure we cover all dates we have in the fact table
            and AverageTemperature is not null
        group by date, city, country
    """)

    
    dim_world_temperatures = df_temperature_clean
    
    # Write dim table into S3 in parquet format
    print('Writing temperature dimension table in parquet...')
    
    dim_world_temperatures.write.partitionBy("country").parquet((output_data+"dim_world_temperatures"),'overwrite')
    print('Dim temperatures table created in parquet')
    


def run_quality_checks():
    """Perform quality checks against each the fact/dimension tables created:
        - count records to ensure completeness
        - display schema and data types
        - raise an error if the count of records is < 0
    """
    tables = ['fact_immigration', 'dim_time', 'dim_airports', 'dim_cities_demographics', 'dim_world_temperatures']
    for i in tables:
        print(i)
        path = "2016_04/"+ i
        #print(path)
        count_records = spark.read.parquet(path).count()
        #print(count_records)
        if count_records < 1:
            raise ValueError(f"Data quality check failed. {i} contained 0 rows")
        print(f"Data quality check on table {i} passed with {count_records} records") 
        print("Showing schema:")
        spark.read.parquet(path).printSchema()
    

def main():
    """Main function that executes all processing functions and quality checks on final tables
    """
    
    spark = create_spark_session()
    output_data = "s3a://udacity-dend-capstone/"
    
    
    input_data = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    process_immigration_data(spark=spark, input_data=input_data, output_data=output_data)
    
    input_data = 'airport-codes_csv.csv'
    process_airports_data(spark=spark, input_data=input_data, output_data=output_data)
    
    input_data = 'us-cities-demographics.csv'
    process_cities_data(spark=spark, input_data=input_data, output_data=output_data)
    
    input_data = '../../data2/GlobalLandTemperaturesByCity.csv'
    process_temperature_data(spark=spark, input_data=input_data, output_data=output_data)
    
    run_quality_checks()
    
    
if __name__ == "__main__":
    main()
