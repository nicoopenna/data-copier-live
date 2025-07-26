import os
DB_DETAILS = {
    'dev': {
        'SOURCE_DB': {
            'DB_TYPE': 'mysql',
            'DB_HOST': '0.0.0.0',
            "DB_PORT": "3307",
            'DB_NAME': 'retail_db',
            'DB_USER': os.environ.get('SOURCE_DB_USER'),
            'DB_PASS': os.environ.get('SOURCE_DB_PASS'),
        },
        'TARGET_DB': {
            'DB_TYPE': 'postgres',
            'DB_HOST': '0.0.0.0',
            "DB_PORT": "5433",
            'DB_NAME': 'retail_db',
            'DB_USER': os.environ.get('TARGET_DB_USER'),
            'DB_PASS': os.environ.get('TARGET_DB_PASS'),
        },
    }
}