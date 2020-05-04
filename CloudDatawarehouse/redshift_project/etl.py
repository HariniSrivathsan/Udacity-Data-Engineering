import configparser
import psycopg2
from tqdm import tqdm
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Inserting data into staging tables from raw files
    :param cur:
    :param conn:
    :return:
    """
    for query in tqdm(copy_table_queries, total=len(copy_table_queries), unit='queries'):
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserting target tables with data from staging tables
    :param cur:
    :param conn:
    :return:
    """
    for query in tqdm(insert_table_queries, total=len(insert_table_queries), unit='queries'):
        print(f'Processing query: {query}')
        cur.execute(query)
        conn.commit()


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