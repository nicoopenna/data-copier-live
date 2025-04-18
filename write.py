from psycopg2.extras import execute_batch
from util import get_connection


def write_table(db_details, table_name, data, column_names):
    target_db = db_details['TARGET_DB']

    connection = get_connection(db_type=target_db['DB_TYPE'],
                                db_name=target_db['DB_NAME'],
                                db_host=target_db['DB_HOST'],
                                db_port=target_db['DB_PORT'],
                                db_user=target_db['DB_USER'],
                                db_pass=target_db['DB_PASS'])
    cursor = connection.cursor()
    query = f"INSERT INTO {table_name} ({column_names}) VALUES (%s, %s)"
    execute_batch(cursor,query, data)