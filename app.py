import sys
import os
from config import DB_DETAILS
from util import get_tables
import mysql.connector as myc
import pandas as pd

def main():
    """
    Program takes at least one argument
    """
    env = sys.argv[1]
    db_details = DB_DETAILS[env]

    conn = myc.connect(user=db_details["SOURCE_DB"]["DB_USER"],
                password=db_details["SOURCE_DB"]["DB_PASS"],
                database=db_details["SOURCE_DB"]["DB_NAME"],
                host=db_details["SOURCE_DB"]["DB_HOST"],
                port=db_details["SOURCE_DB"]["DB_PORT"]
                )
    cursor = conn.cursor()
    cursor.execute("select * from categories")
    df = pd.DataFrame(data=cursor.fetchall(),columns=cursor.column_names)
    print(df.head())
    tables = get_tables('table_list')



if __name__ == '__main__':
    main()