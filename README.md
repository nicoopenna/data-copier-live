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

## 📦 Pipeline Architecture

## 📊 Pipeline Architecture
<details>
<summary>Click to view diagram</summary>

![Data Flow](https://www.mermaidchart.com/raw/e0f73889-bafc-4e4e-828f-725afcd75634?theme=light&version=v0.1&format=svg)
</details>