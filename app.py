import sys
import os
from config import DB_DETAILS




def main():
    """
    Program takes at least one argument
    Min 49
    """
    env = sys.argv[1]
    db_details = DB_DETAILS[env]
    print(db_details)


if __name__ == '__main__':
    main()