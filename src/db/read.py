from src.utils.util import get_connection
import logging


logger = logging.getLogger(__name__)


def read_table(db_details, table_name, limit=0):
    """
    Read data from a database table with basic logging
    Args:
        db_details: Database connection details
        table_name: Table to query
        limit: Max rows to return (0=all)
    Returns:
        (data_rows, column_names)
    """
    logger.debug(f"Starting read from {table_name} (limit: {limit})")
    source_db = db_details['SOURCE_DB']
    conn = None

    try:
        conn = get_connection(
            db_type=source_db['DB_TYPE'],
            db_name=source_db['DB_NAME'],
            db_host=source_db['DB_HOST'],
            db_port=source_db['DB_PORT'],
            db_user=source_db['DB_USER'],
            db_pass=source_db['DB_PASS']
        )

        query = f"SELECT * FROM {table_name}"
        if limit > 0:
            query += f" LIMIT {limit}"

        with conn.cursor() as cur:
            cur.execute(query)
            data = cur.fetchall()
            cols = cur.column_names
            logger.debug(f"Retrieved {len(data)} rows from {table_name}")
            return data, cols

    except Exception as e:
        logger.error(f"Failed reading {table_name}: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()
            logger.debug("Connection closed")