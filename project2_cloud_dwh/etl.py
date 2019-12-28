import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from time import time


def load_staging_tables(cur, conn):
    """
    Load data from log files in S3 to staging tables in Redshift
    """
    for query in copy_table_queries:
        print('Loading data to staging tables...: '+query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data from staging tables in Redshift to final Fact/Dimension tables in Redshift
    """
    for query in insert_table_queries:
        print('Inserting data into final tables...: '+query)
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function to connect to Amazon Redshift cluster and insert data to Redshift staging and final tables via functions above.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    print('Connecting to Redshift cluster')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Connected to Redshift cluster')
    
    print('Loading into staging tables')
    t_load_start = time()
    load_staging_tables(cur, conn)
    loading_time = time() - t_load_start
    
    print('Inserting into final tables')
    t_insert_start = time()
    insert_tables(cur, conn)
    inserting_time = time() - t_insert_start
    
    print("=== LOADING DONE IN: {0:.2f} sec\n".format(loading_time))
    print("=== INSERTING DONE IN: {0:.2f} sec\n".format(inserting_time))

    conn.close()


if __name__ == "__main__":
    main()