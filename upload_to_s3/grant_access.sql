DROP USER IF EXISTS 'airflow'@'%';
CREATE USER 'airflow'@'%' IDENTIFIED BY 'airflow';
GRANT ALL PRIVILEGES ON airflow.* TO 'airflow'@'%';
FLUSH PRIVILEGES;
