import sys
import os
import logging
from datetime import datetime
from read import read_table
from util import get_tables, load_db_details
from write import write_table

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def main():
    """
    Data pipeline main execution with robust error handling
    Usage: python main.py [environment]
    Example: python main.py dev
    """
    try:
        # Validate arguments
        if len(sys.argv) < 2:
            raise ValueError("Missing environment argument (dev/prod)")

        env = sys.argv[1]
        logger.info(f"Starting pipeline execution for {env} environment")
        start_time = datetime.now()

        # Load configuration
        db_details = load_db_details(env)
        tables = get_tables('table_list')

        if tables.empty:
            logger.warning("No tables found in table_list configuration")
            return

        logger.info(f"Tables to process: {', '.join(tables['table_name'])}")

        # Process tables
        success_count = 0
        for table in tables['table_name']:
            try:
                logger.info(f"Processing table: {table}")
                table_start = datetime.now()

                data, column_names = read_table(
                    db_details=db_details,
                    table_name=table
                )

                if not data:
                    logger.warning(f"No data found for table {table}")
                    continue

                write_table(
                    db_details=db_details,
                    table_name=table,
                    data=data,
                    column_names=column_names
                )

                success_count += 1
                logger.info(
                    f"Completed {table} in {datetime.now() - table_start} "
                    f"({len(data)} rows)"
                )

            except Exception as e:
                logger.error(f"Failed processing table {table}: {str(e)}", exc_info=True)
                continue

        # Summary report
        total_time = datetime.now() - start_time
        logger.info(
            f"Pipeline completed. {success_count}/{len(tables['table_name'])} "
            f"tables processed in {total_time}"
        )

    except Exception as e:
        logger.critical(f"Pipeline failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()