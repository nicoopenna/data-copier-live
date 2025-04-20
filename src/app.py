#!/usr/bin/env python3
"""
Data Pipeline Main Module - MySQL to PostgreSQL Migration
"""

import sys
import os
import logging
from datetime import datetime
from pathlib import Path
from src.db.read import read_table
from src.utils.util import get_tables, load_db_details
from src.db.write import write_table


# Configure logging
def setup_logging():
    """Configure logging with both file and console output"""
    log_dir = Path(__file__).parent.parent / 'logs'
    log_dir.mkdir(exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_dir / 'data_pipeline.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


logger = setup_logging()


def validate_environment(env):
    """Validate the provided environment argument"""
    valid_envs = ['dev', 'prod', 'test']
    if env not in valid_envs:
        raise ValueError(f"Invalid environment '{env}'. Must be one of: {', '.join(valid_envs)}")


def process_table(db_details, table_name):
    """Process a single table migration"""
    logger.info(f"Starting migration for table: {table_name}")
    start_time = datetime.now()

    try:
        # Read data from source
        data, column_names = read_table(db_details, table_name)

        if not data:
            logger.warning(f"No data found for table {table_name}")
            return False

        # Write data to target
        write_table(db_details, table_name, data, column_names)

        duration = datetime.now() - start_time
        logger.info(
            f"Successfully migrated {table_name} "
            f"({len(data)} rows in {duration.total_seconds():.2f}s)"
        )
        return True

    except Exception as e:
        logger.error(f"Failed to migrate table {table_name}: {str(e)}", exc_info=True)
        return False


def main():
    """
    Data pipeline main execution with robust error handling
    Usage: python app.py [environment]
    Example: python app.py dev
    """
    try:
        # Validate arguments
        if len(sys.argv) < 2:
            raise ValueError("Missing environment argument (dev/prod/test)")

        env = sys.argv[1].lower()
        validate_environment(env)

        logger.info(f"Starting data pipeline execution for {env} environment")
        pipeline_start = datetime.now()

        # Load configuration
        db_details = load_db_details(env)
        tables = get_tables(Path(__file__).parent.parent / 'src/config/table_list')

        if tables.empty:
            logger.warning("No tables found in table_list configuration")
            return

        logger.info(f"Tables to migrate: {', '.join(tables['table_name'])}")

        # Process tables
        success_count = 0
        for table_name in tables['table_name']:
            if process_table(db_details, table_name):
                success_count += 1

        # Summary report
        total_duration = datetime.now() - pipeline_start
        logger.info(
            f"Pipeline completed. Success: {success_count}/{len(tables['table_name'])} "
            f"tables in {total_duration.total_seconds():.2f} seconds"
        )

        # Exit with appropriate status code
        sys.exit(0 if success_count == len(tables['table_name']) else 1)

    except Exception as e:
        logger.critical(f"Pipeline execution failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()