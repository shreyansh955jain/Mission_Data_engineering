

#AWS Access And Secret key
aws_access_key = "1POIjFaPsBIwQkrK+gOG0BMvGcuh3I0Q6NlsfAuJBUA="
aws_secret_key = "yJv03o40qM3O8zQyJeHurNwUUrB8ai4nP6rl1hoREQVfDS8Ef/oV4AvzbV/65OEN"
bucket_name = "mission-de-project"
s3_customer_datamart_directory = "customer_data_mart"
s3_sales_datamart_directory = "sales_data_mart"
s3_source_directory = "sales_data/"
s3_error_directory = "sales_data_error/"
s3_processed_directory = "sales_data_processed/"


#Database credential
# MySQL database connection properties
database_name = "mission_de_source"
url = f"jdbc:mysql://localhost:3306/{database_name}"
properties = {
    "user": "root",
    "password": "T/1JVOYj1yFV4+AtlDQ9fQ==",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Table name
customer_table_name = "customer"
product_staging_table: str = "product_staging_table"
product_table = "product"
sales_team_table = "sales_team"
store_table = "store"

#Data Mart details
customer_data_mart_table = "customers_data_mart"
sales_team_data_mart_table = "sales_team_data_mart"

# Required columns
mandatory_columns = ["customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost"]


# File Download location
local_directory = "/home/shreyansh-jain/PycharmProjects/MIssion_DataEngineering/data_modeling_dir/file_from_s3/"
customer_data_mart_local_file = "/home/shreyansh-jain/PycharmProjects/MIssion_DataEngineering/data_modeling_dir/customer_data_mart/"
sales_team_data_mart_local_file = "/home/shreyansh-jain/PycharmProjects/MIssion_DataEngineering/data_modeling_dir/sales_team_data_mart/"
sales_team_data_mart_partitioned_local_file = "/home/shreyansh-jain/PycharmProjects/MIssion_DataEngineering/data_modeling_dir/sales_partition_data/"
error_folder_path_local = "/home/shreyansh-jain/PycharmProjects/MIssion_DataEngineering/data_modeling_dir/error_files/"




#mysql-connectivity
host="localhost"
user="root"
password = "T/1JVOYj1yFV4+AtlDQ9fQ=="
database="mission_de_source"
product_stage_table="product_staging_table"



key = "youtube_project"
iv = "youtube_encyptyo"
salt = "youtube_AesEncryption"