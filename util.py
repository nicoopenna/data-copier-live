import pandas as pd
from mysql import connector as mc
from mysql.connector import errorcode as ec
from config import DB_DETAILS
from psycopg2 import connect


def get_tables(path):
    tables = pd.read_csv(path, sep=':')
    return tables.query('to_be_loaded == "yes"')

def load_db_details(env):
    return DB_DETAILS[env]

def get_mysql_conn(db_host, db_port, db_name, db_user, db_pass):
    try:
        connection = mc.connect(user=db_user,
                       password=db_pass,
                       database=db_name,
                       host=db_host,
                       port=db_port
                       )
    except mc.Error as error:
        if error.errno == ec.ER_ACCESS_DENIED_ERROR:
            print("Invalid credentials")
        else:
            print(error)
    return connection


def get_postgres_conn(db_host, db_port, db_name, db_user, db_pass):
    try:
        connection = connect(user=db_user,
                       password=db_pass,
                       database=db_name,
                       host=db_host,
                       port=db_port
                       )
    except mc.Error as error:
        if error.errno == ec.ER_ACCESS_DENIED_ERROR:
            print("Invalid credentials")
        else:
            print(error)
    return connection


def get_connection(db_type, db_host, db_port, db_name, db_user, db_pass):
    if db_type == 'mysql':
        connection = get_mysql_conn(db_host = db_host,
                                    db_port = db_port,
                                    db_name = db_name,
                                    db_user = db_user,
                                    db_pass = db_pass)
    if db_type == 'postgres':
        connection = get_postgres_conn(db_host=db_host,
                                    db_port=db_port,
                                    db_name=db_name,
                                    db_user=db_user,
                                    db_pass=db_pass)
    return connection
