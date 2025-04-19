from psycopg2.extras import execute_values
from psycopg2 import sql
from util import get_connection
import logging

logger = logging.getLogger(__name__)


def write_table(db_details, table_name, data, column_names, batch_size=10000):
    """
    High-performance batch insert for data pipelines.

    Args:
        db_details: Database config with TARGET_DB dictionary
        table_name: Target table name
        data: List of tuples with data rows
        column_names: Column names as string tuple ('col1','col2') or list
        batch_size: Rows per batch (default: 10,000)
    """
    if not data:
        logger.warning(f"Skipping empty dataset for {table_name}")
        return

    try:
        # Fast column name sanitization
        if isinstance(column_names, str):
            cols = [col.strip(" '\"") for col in column_names.strip()[1:-1].split(',')]
        else:
            cols = [str(col).strip(" '\"") for col in column_names]

        if not cols:
            raise ValueError("Column names required")

        target_db = db_details['TARGET_DB']

        with get_connection(
                db_type=target_db['DB_TYPE'],
                db_name=target_db['DB_NAME'],
                db_host=target_db['DB_HOST'],
                db_port=target_db['DB_PORT'],
                db_user=target_db['DB_USER'],
                db_pass=target_db['DB_PASS']
        ) as conn:
            with conn.cursor() as cur:
                logger.debug(f"Starting write to {table_name} (columns: {column_names})")
                # Injection-safe query construction
                query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                    sql.Identifier(table_name),
                    sql.SQL(',').join(map(sql.Identifier, cols))
                )

                execute_values(
                    cur,
                    query.as_string(conn),
                    data,
                    page_size=batch_size
                )
                conn.commit()

                logger.debug(f"Inserted {len(data)} rows into {table_name}")

    except Exception as e:
        logger.error(f"Failed on {table_name}: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
        raise