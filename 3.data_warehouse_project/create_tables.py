import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

"""
-drops tables that are already created.
-comes in handy when you need to reset your data tables to fix or debug issues.
"""
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

"""
-executes the create table commands from your sql_queries.py file to generate the corresponding tables.
-establishes connection by using dwh.cfg file for credentials and endpoints to perform ETL process.
-etl process involves loading data into the staging tables and inserting it into analytics tables.
"""
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

"""
-entry point of script.
-drops tables when a reset is needed.
creates the required tables for the Redshift dwh.
-closes connection.
"""
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()