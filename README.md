# Database Migration Pipeline: MySQL → PostgreSQL

![Python](https://img.shields.io/badge/Python-3.9+-blue)
![MySQL](https://img.shields.io/badge/MySQL-5.6-orange)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-blueviolet)
![Docker](https://img.shields.io/badge/Docker-Containers-lightgrey)

A high-performance data pipeline that extracts data from MySQL, transforms it in memory, and loads it into PostgreSQL. Designed for reliability and scalability.

## 🛠 Key Technical Features

- **Batched Data Processing**: Handles large datasets efficiently (10k rows/batch)
- **Connection Pooling**: Managed DB connections with automatic cleanup
- **SQL Injection Protection**: Uses `psycopg2.sql` for safe query building
- **Production-Grade Logging**: Detailed execution tracking at DEBUG level
- **Container-Ready**: Dockerized services with health checks


## 📊 Pipeline Architecture
![Data Flow Diagram](./assets/diagram.svg)

## How to run?
1. Create .env file with 
    - SOURCE_DB_USER=retail_user
    - SOURCE_DB_PASS
    - SOURCE_DB_ROOT_PASS
    - TARGET_DB_PASS
    - TARGET_DB_USER
2. Run docker compose up
3. Select tables in table_list