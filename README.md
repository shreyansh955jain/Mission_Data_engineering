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
![architecture](https://github.com/user-attachments/assets/1657a758-d8bf-4eb0-893a-4f815d3a0e7a)

(Include your ER diagram image here if available.)

![database_schema drawio](https://github.com/user-attachments/assets/c31f0ccf-609c-41af-b251-2e582d2e65be)


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


Screen_shots For refrence:-

![Screenshot from 2025-04-03 10-32-24](https://github.com/user-attachments/assets/e342b50b-4a35-4218-a5b1-a9335ce0cd90)

![Screenshot from 2025-04-03 10-32-42](https://github.com/user-attachments/assets/e8aa9189-953b-45c2-b10d-77e920da9d54)

![Screenshot from 2025-04-03 10-32-00](https://github.com/user-attachments/assets/6aa40f6d-cf23-43c4-a68a-10d002d34193)


![Screenshot from 2025-04-03 10-34-27](https://github.com/user-attachments/assets/d28e74eb-f71a-419d-85b0-3fc043c7f438)

![Screenshot from 2025-04-03 10-35-08](https://github.com/user-attachments/assets/3e4ddfc4-898b-40e3-8762-a92f3fae12e0)


![Screenshot from 2025-04-03 10-35-53](https://github.com/user-attachments/assets/79ab6a85-21c6-4e49-8b1a-5351be53e053)


![Screenshot from 2025-04-03 10-36-28](https://github.com/user-attachments/assets/6d49c67a-8c18-436a-9724-abfa4d839df0)

![Screenshot from 2025-04-03 10-36-48](https://github.com/user-attachments/assets/d70ce675-90ab-4616-879f-8a282c6ac428)

![Screenshot from 2025-04-03 10-38-03](https://github.com/user-attachments/assets/88f9ccc5-3206-4aa2-94e3-2b88d077b625)


![Screenshot from 2025-04-03 10-38-18](https://github.com/user-attachments/assets/cfd9777b-bccd-42c6-9446-72d6a30b055e)

![Screenshot from 2025-04-03 10-38-43](https://github.com/user-attachments/assets/3401184c-cb88-46ce-93e1-043983bc5054)



