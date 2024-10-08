import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

"""
-Loading data from S3 into Redshift staging tables(events, songs)
-Funciton uses copy command in the copy_table_queries
"""
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

"""
-Inserting data from staging tables into corresponding analytics tables.
-Uses insert statements from insert command queries to load data into tables.
"""
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

"""
-main entrypoint of application.
-establishes connection with Redshift cluster and performs ETL process to load data.
-function reads dwh cfg files to establish connection to Redshift cluster.
-loads data into proper staging tables.
-inserts data from staging tables into proper analytics tables.
"""
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()