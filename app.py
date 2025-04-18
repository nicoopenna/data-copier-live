import sys
import os
from read import read_table
from util import get_tables, load_db_details
import pandas as pd

def main():
    """
    Program takes at least one argument
    """
    env = sys.argv[1]
    db_details = load_db_details(env)
    tables = get_tables('table_list')
    for table in tables:
        data, column_names = read_table(db_details=db_details,
                                        table_name=table)




if __name__ == '__main__':
    main()