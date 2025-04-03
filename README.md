# Mission Data Engineering

## Overview
Mission Data Engineering is a hands-on project focused on real-time data processing using Apache Spark and AWS S3. The project is designed to simulate a production-ready data engineering environment, providing deep insights into real-world data workflows.

## Project Goals
- Efficiently process large volumes of data using Apache Spark.
- Implement data transformation pipelines for structured analysis.
- Store and retrieve data using AWS S3.
- Optimize SQL queries for improved performance.
- Automate workflows with CI/CD pipelines.

## Prerequisites
To set up and run this project, ensure you have the following:
- A laptop with at least **4GB RAM and an i3 processor** (recommended: **8GB RAM and an i5 processor**).
- Apache Spark installed locally (setup instructions provided in the documentation).
- **Python 3.10.11** (recommended over older versions like 3.6 or 3.9).
- **PyCharm** as the IDE.
- **MySQL Workbench** installed.
- A **GitHub account** (optional but recommended).
- An **AWS account** with S3 full access.
- Understanding of **Apache Spark, SQL, and Python**.

## Project Structure
```
my_project/
├── docs/
│   └── readme.md
├── resources/
│   ├── __init__.py
│   ├── dev/
│   │    ├── config.py
│   │    └── requirement.txt
│   ├── qa/
│   │    ├── config.py
│   │    └── requirement.txt
│   ├── prod/
│   │    ├── config.py
│   │    └── requirement.txt
│   ├── sql_scripts/
│   │    └── table_scripts.sql
├── src/
│   ├── main/
│   │    ├── __init__.py
│   │    ├── delete/
│   │    │      ├── aws_delete.py
│   │    │      ├── database_delete.py
│   │    │      └── local_file_delete.py
│   │    ├── download/
│   │    │      └── aws_file_download.py
│   │    ├── move/
│   │    │      └── move_files.py
│   │    ├── read/
│   │    │      ├── aws_read.py
│   │    │      └── database_read.py
│   │    ├── transformations/
│   │    │      ├── jobs/
│   │    │      │     ├── customer_mart_sql_transform_write.py
│   │    │      │     ├── dimension_tables_join.py
│   │    │      │     ├── main.py
│   │    │      │     └── sales_mart_sql_transform_write.py
│   │    ├── upload/
│   │    │      └── upload_to_s3.py
│   │    ├── utility/
│   │    │      ├── encrypt_decrypt.py
│   │    │      ├── logging_config.py
│   │    │      ├── s3_client_object.py
│   │    │      ├── spark_session.py
│   │    │      └── my_sql_session.py
│   │    ├── write/
│   │    │      ├── database_write.py
│   │    │      └── parquet_write.py
├── test/
│   ├── scratch_pad.py
│   └── generate_csv_data.py
```

## How to Run the Project in PyCharm
1. Open PyCharm.
2. Clone or upload the project from GitHub.
3. Open the terminal in PyCharm.
4. Navigate to the virtual environment and activate it:
   ```sh
   cd venv
   cd Scripts
   activate   # (use ./activate if the command doesn't work)
   ```
5. Create `main.py` as per the project structure.
6. Set up an AWS user with S3 full access and configure the access keys in the config file.
7. Run `main.py` using the **green play button** in PyCharm.
8. If everything is set up correctly, the program will run smoothly. Otherwise, debug and retry.

## Project Architecture
- **Data Sources:** AWS S3, MySQL databases.
- **Processing Engine:** Apache Spark.
- **Data Storage:** AWS S3.
- **Workflow Automation:** Airflow, CRON jobs.
- **Deployment:** Integrated with Azure CI/CD pipeline.

## Database ER Diagram
(Include your ER diagram image here if available.)

## Key Achievements
- Successfully managed **100GB of daily data**.
- Optimized Spark jobs using **caching and broadcast joins**.
- Designed and implemented an **incentive program** for sales performance.
- Automated data pipelines using **Airflow and CRON jobs**.
- Implemented a **customer engagement strategy** to boost retention.

## Resume Highlights
- Led a **data engineering project in a retail environment** using Apache Spark, Python, SQL, and AWS S3.
- Built **structured data models** with dimension and fact tables for better analysis.
- Designed an **incentive program** to motivate sales teams.
- Handled **large-scale data pipelines**, processing ~100GB of data daily.
- Optimized Spark performance through **efficient memory management techniques**.

## Interview Preparation Guide
During an interview, you can elaborate on:
- How Apache Spark was used for efficient data processing.
- How SQL and Python were integrated for data transformations.
- The role of AWS S3 in the project.
- The strategies implemented for **performance optimization**.
- The automation techniques used for scheduling jobs.

## Conclusion
Mission Data Engineering is a **comprehensive, real-world project** that showcases the power of data engineering techniques. It provides a structured approach to handling large-scale data and demonstrates expertise in modern data processing technologies.

For further inquiries, refer to the **project documentation** or reach out via GitHub.

---
**Happy Coding!** 🚀

