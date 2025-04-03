import shutil
from datetime import datetime
from pyspark.sql.connect.functions import concat_ws
from pyspark.sql.functions import lit, expr
from pyspark.sql.types import StructField, IntegerType, StringType, DateType, FloatType
from pyspark.sql.types import StructType

from src.main.delete.local_file_delete import delete_local_file
from src.main.download.aws_file_download import S3FileDownloader
from src.main.move.move_files import move_s3_to_s3
from src.main.read.aws_read import S3Reader
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.transformations.jobs.dimension_tables_join import dimesions_table_join
from src.main.transformations.jobs.sales_mart_sql_transform_write import sales_mart_calculation_table_write
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.encrypt_decrypt import *
from src.main.utility.logging_config import *
from src.main.utility.my_sql_session import *
from src.main.utility.s3_client_object import *
from src.main.utility.spark_session import spark_session
from src.main.write.parquet_writer import ParquetWriter

#logger = logging.getLogger(name)

# Get S3 client
aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key

print(f'Encryption Access key : {aws_access_key}')
print(f'Encryption Security key: {aws_secret_key}')


s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()

# Now you can use s3_client for your S3 operations
response = s3_client.list_buckets()
print(f'Response share by s3 bucket connectivity ',' : {response}')
logger.info("List of Buckets: %s", response['Buckets'])



#Step1- Check the file exists or not in the dir location from S3 buket so we can process the file.
#Step2- Incase file exists then check if the same file is present in staging area
#step3 status as A(active). if so then don't delete and try to rerun
#else give an error and not process the next files

# Because if file shows inactive it means file processed successfully.

csv_files = [file for file in os.listdir(config.local_directory) if file.endswith(".csv")]
# create the list of files.
# example ["xvz.csv", "ABC.csv"]


# mysql connection


# Create a cursor object to execute queries
connection = get_mysql_connection()
cursor = connection.cursor()

######################## check the last run file processed successfully or Not #################################


# this step follows a check validation for last run processed was completed or not.

total_csv_files = []
if csv_files:
    for file in csv_files: #list of files fetched from local location.
        total_csv_files.append(file)
        print("1",file)
    print(str(total_csv_files)[1:-1])
    statement = f"select distinct file_name from {config.database}.{config.product_stage_table} " \
                f"where file_name in ({str(total_csv_files)[1:-1]}) and status='A' "
    print(statement)
    logger.info(f"dynamically statement created: {statement}")
    cursor.execute(statement)
    data = cursor.fetchall()
    # print(f'List of files failed in last process: {data}')
    if data:
        print("Your last run has failed please check")
    else:
        print("NO record found to process")

print("Your last run was succesfull")

#Now we have stored all the Files that had been lifted for process in last run in the Data variable.


########################### Read file from S3 ########################################################

s3_absolute_file_path = []  # Define it before the try block

try:
    s3_reader = S3Reader()  # object from aws_read.py
    #bucket name should come from table.
    folder_path = config.s3_source_directory
    s3_absolute_file_path = s3_reader.list_files(s3_client,config.bucket_name,folder_path)
    print(s3_absolute_file_path)
    logger.info("Absolute path on s3 bucket for csv file %s ",s3_absolute_file_path)
    if not s3_absolute_file_path:
        logger.info(f"No files available at {folder_path}")
        raise Exception("No data available in process")

except Exception as e:
    logger.error(f"An error occurred: {e}")


# Once We recive the list of file name from S3 bucket in s3_absolute_file_path.

# Now we need to Download these files from S3 bucket.

bucket_name = config.bucket_name
local_directory = config.local_directory


prefix = f"s3://{bucket_name}/"
file_paths = [url[len(prefix):] for url in s3_absolute_file_path]

# File path for exp : ['sales_data/grant_access.sql', 'sales_data/hiring.csv', 'sales_data/products.csv']
logger.info(f"file path available on s3 under {bucket_name} bucket and folder name is {local_directory}")
logger.info(f'File path available on s3 bucket : {file_paths}')

try:
    downloader = S3FileDownloader(s3_client, bucket_name, local_directory)
    downloader.download_files(file_paths)
except Exception as e:
    logger.error(f"An error occurred while downloading : {e}")
    sys.exit()


print("******************************** Checks and Validation ***********************************")

# Now Once we have downloaded the files , We need to Check the validation
# weather the files are correct the process.
# it's a csv or not
# additionally need to check the schema of the csv files


# get a list of all files in the local directory
all_files = os.listdir(local_directory)
logger.info(f'List of all files in local directory after download: {all_files}')

# exp-List of all files in local directory after download: ['hiring.csv', 'grant_access.sql', 'products.csv']

# filters files with ".csv" in the local directory and put other files in error_files
if all_files:
    csv_files = []
    error_files = []
    for file in all_files:
        if file.endswith(".csv"):
            csv_files.append(os.path.abspath(os.path.join(local_directory, file)))
        else:
            error_files.append(os.path.abspath(os.path.join(local_directory, file)))
    # check the csv file is empty the no csv to process
    if not csv_files: # validation check's
        logger.info("NO csv file found to process")
        raise Exception("No data available in process")
    else:
        logger.info(f"csv file available to process {csv_files}")

# validation check's
else:
    logger.info("Their is no data to process")
    raise Exception("No data available in process")

######Make the csv lines convert into a list of comma separated ############
csv_files1 = str(csv_files)[1:-1]

logger.info("*****************************liST OF CSV FILE ***************************************")

logger.info("list of csv files %s",csv_files1)

logger.info("*****************************creating spark session *********************************")

spark = spark_session()

logger.info("***************************** spark session created *********************************")

# check the requirement column in the schema of csv files
# if not required columns keep it in a list or error_files
# Else union all the data into one dataframe

logger.info("*************************** checking Schema for data loaded in s3 *********************")


# This below step for checking if any required column is missing in the CSV file location then move that file into error file location!!!
# point 2 if the file contain any extra column that is file... we will handle those in next steps.

correct_schema = []
for data_file in csv_files:
    try:
        correct_path = "/home/shreyansh-jain/PycharmProjects/MIssion_DataEngineering/data/"
        df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(data_file)
        schema = df.columns
        print(schema)
        print(schema)
        logger.info(f"Schema of {data_file} is {df}")
        logger.info(f"Mandatory columns are {config.mandatory_columns}")
        required_columns = config.mandatory_columns
        missing_columns = []
        for column in required_columns:
            if column not in schema:
                missing_columns.append(column)
        if missing_columns:
            logger.info(f"Missing columns in {data_file}: {missing_columns}")
            error_files.append(data_file)
        else:
            logger.info(f'No missing columns in {data_file}')
            correct_schema.append(data_file)

    except Exception as e:
        logger.error(f"An error occurred while processing {data_file}: {e}")


# validation check's
if not correct_schema:
    logger.info("No csv file found to process")
    raise Exception("No data available in process")
else:
    logger.info(f"csv file available to process {correct_schema}")

# MOve the data to error directory on local.

print("******************************** Move the data to error dir ***********************************")

if error_files:
    for file in error_files:
        if os.path.exists(file):
            file_name = os.path.basename(file)
            destination_path = os.path.join(config.error_folder_path_local, file_name)
            shutil.move(file, destination_path)  # To move the file from source to destination file.
            logger.info(f"Moved '{file_name}'from s3 file path to '{destination_path}'.")

            source_prefix = config.s3_source_directory
            destination_prefix = config.s3_error_directory

            message = move_s3_to_s3(s3_client,bucket_name,source_prefix,destination_prefix,file_name)
            logger.info(f"message : {message}")

        else:
            logger.error(f"{file_paths} does not exist.")

else:
    logger.info("*********************** No error files found to move ***********************************")


# Before running the process
# Stage table needs to be updated with status as Active(A) or Inactive(I)

logger.info(f'********** updating the product_staging_table that we have started the process ***************')

insert_statements = []
db_name = config.database_name
current_date = datetime.now()
formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")

if correct_schema:
    for file in correct_schema:
        file_name = os.path.basename(file)
        file_path = os.path.join(config.local_directory, file_name)
        file_size = os.path.getsize(file_path)
        file_status = 'A'
        insert_statement = f"insert into {db_name}.{config.product_stage_table} (file_name,file_location,created_date, status) values ('{file_name}', '{file_name}', '{formatted_date}','{file_status}')"
        insert_statements.append(insert_statement)
        logger.info(f"insert statement for {file_name} : {insert_statement}")

    logger.info(f"insert statements created for staging table : {insert_statements}")
    logger.info("********************** Connecting with mySQL server *******************************************")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("********************** Connected with mySQL server *******************************************")
    for insert_statement in insert_statements:
        cursor.execute(insert_statement)
        connection.commit()
        logger.info(f"insert statement executed : {insert_statement}")
    cursor.close()
    connection.close()
    logger.info("********************** Disconnected from mySQL server *******************************************")
else:
    logger.error("***************************** No csv file found to process *********************************")
    raise Exception("***************************** No data available in process ******************************")


logger.info("***************************** Staging table updated successfully *********************************")

logger.info("***************************** Fixing extra column coming from source *********************************")
schema= StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", FloatType(), True),
    StructField("addition_column", StringType(), True)
])



final_df_to_process = spark.createDataFrame([], schema = schema)

final_df_to_process.show()

print("final_df_to_process :",{"final_df_to_process":final_df_to_process})


# create a new column with concatenated values of extra columns

for data in correct_schema:
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(data)
    data_schema = df.columns
    extra_column = list(set(data_schema) - set(config.mandatory_columns))
    logger.info(f'Extra columns present at source is : {extra_column}')
    if extra_column:
        data_df = df.withColumn("additional_column", concat_ws(",", *extra_column)) \
            .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity",
                    "total_cost", "additional_column")
        logger.info(f"processed {data} and added 'additional_column' column")
    else:
        data_df = df.withColumn("addition_column", lit(None).cast(StringType())) \
            .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity",
                    "total_cost", "addition_column")
        logger.info(f"processed {data} and added 'addition_column' column")

    final_df_to_process = final_df_to_process.union(data_df)
# final_df_to_process = data_df

logger.info(
    "******************************** final_df_to_process  from source which will be going to processing *********************************")
final_df_to_process.show()

# Enrich the data from all dimension table
# Also create a datamart for sales_team and their incentive , address and all
# another datamart for customer who bought how inside that
# there should be a store_id segregation
# Read the data from parquet and generate a csv_file
# in Which there will be a sales_person_name, Sales_person_store_id
# sales_person_total_billing_done_for_earth_month, total_incentive .




# DataWarehouse -> Central repository , Datamart -> specify DataWarehouse like customer and sales.


# Connection with DatabaseReader
database_client = DatabaseReader(config.url,config.properties)

#Creating DF for ALL Tables and load the data

#Customer Table
logger.info("*************************** Loading Customer table into customer_table_df *****************************")
customer_table_df  = database_client.create_dataframe(spark,config.customer_table_name)

# product table
logger.info("*************************** Loading Product table into product_table_df *****************************")
product_table_df  = database_client.create_dataframe(spark,config.product_table)

#product_staging_table table
logger.info("*************************** Loading Product_staging table into product_staging_table_df *****************************")
product_staging_table_df  = database_client.create_dataframe(spark,config.product_staging_table)

#sales_team table
logger.info("*************************** Loading Sales_team table into sales_team_table_df *****************************")
sales_team_table_df  = database_client.create_dataframe(spark,config.sales_team_table)

#store table
logger.info("*************************** Loading Store table into store_table_df *****************************")
store_table_df  = database_client.create_dataframe(spark,config.store_table)


s3_customer_store_sales_df_join = dimesions_table_join(final_df_to_process, customer_table_df, store_table_df, sales_team_table_df)

# Final Enriched Data
logger.info("********************* Final Enriched Data *********************************")
s3_customer_store_sales_df_join.show()


# Write the customer data into customer data mart in parquet format
# File will be written to Local First
# Move the Raw Data to S3 Bucket For Reporting Tool.
# Write reporting Data into MYSQL table Also

logger.info("******************* Write the data into customer data mart in parquet format *********************")
final_customer_data_mart_df = s3_customer_store_sales_df_join\
                .select("ct.customer_id",
                        "ct.first_name",
                        "ct.last_name",
                        "ct.address",
                        "ct.pincode",
                        "phone_number",
                        "sales_date",
                        "total_cost")

logger.info("******************* Final Data for customer Data Mart *********************")
final_customer_data_mart_df.show()

parquet_writer = ParquetWriter("overwrite","parquet")
parquet_writer.dataframe_writer(final_customer_data_mart_df,config.customer_data_mart_local_file)

logger.info(f"******************* Customer Data Writer to local disk at {config.customer_data_mart_local_file} *********************")

# Move data an S3 bucket for customer_data_mart

logger.info(f"******************* Move data from local to S3 for customer data mart*********************")

s3_uploader = UploadToS3(s3_client)
s3_directory = config.s3_customer_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory,config.bucket_name,config.customer_data_mart_local_file)
logger.info(f"{message}")

#Sales team Data Mart
logger.info("********************* Write the data into sales team data mart in parquet format *********************")
final_sales_team_data_mart_df = s3_customer_store_sales_df_join\
                .select("store_id",
                        "sales_person_id",
                        "sales_person_first_name",
                        "sales_person_last_name",
                        "store_manager_name",
                        "manager_id",
                        "is_manager",
                        "sales_person_address",
                        "sales_person_pincode",
                        "sales_date",
                        "total_cost",
                        expr("SUBSTRING(sales_date,1,7) as sales_month"))


logger.info("********************* Final Data for sales team Data Mart *********************")
final_sales_team_data_mart_df.show()
parquet_writer.dataframe_writer(final_sales_team_data_mart_df,config.sales_team_data_mart_local_file)

logger.info(f"******************* Sales team Data Writer to local disk at {config.sales_team_data_mart_local_file} *********************")

#Move data on s3 bucket for sales_data_mart

s3_directory = config.s3_sales_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory,config.bucket_name,config.sales_team_data_mart_local_file)
logger.info(f"{message}")

# Also writing the data into partitions
final_sales_team_data_mart_df.write.format("parquet")\
                   .option("header","true")\
                   .mode("overwrite")\
                   .partitionBy("sales_month","store_id")\
                   .option("path",config.sales_team_data_mart_partitioned_local_file)\
                   .save()

#Move data on s3 for partitioned folder

s3_prefix = "sales_partitioned_data_mart"
current_epoch = int(datetime.now().timestamp())*1000
for root,dirs, files in os.walk(config.sales_team_data_mart_partitioned_local_file):
    for file in files:
        print(file)
        file_path = os.path.join(root,file)
        # noinspection PyTypeChecker
        destination_path = os.path.relpath(file_path,config.sales_team_data_mart_partitioned_local_file)
        s3_key = f"{s3_prefix}/{current_epoch}/{destination_path}"
        s3_client.upload_file(file_path,config.bucket_name,s3_key)


# calculation for sales team mart
# find out the customer total purchase every month
# write the data into MYSQL table

logger.info("********************* Calculation customer every month purchase amount *********************")
customer_mart_calculation_table_write(final_customer_data_mart_df)
logger.info("********************* Calculation customer every month purchase amount Done *********************")

#calculate for sales team mart
#find out the total sales done by each sales person every month.
#Give the top performance 1% incentive of the total sales of the month
# Rest Sales person will Get nothing
#write the data into MY SQL.

logger.info("******************** Calculation sales person every month billed amount *********************")
sales_mart_calculation_table_write(final_sales_team_data_mart_df)
logger.info("******************** Calculation sales person every month billed amount Done *********************")


########################################### LAST STEP ################################################

# MOve the file on s3 into processed folder and delete the local files
source_prefix = config.s3_source_directory
destination_prefix = config.s3_processed_directory
message = move_s3_to_s3(s3_client,config.bucket_name,source_prefix,destination_prefix)
logger.info(f"message : {message}")

logger.info("***************************** Process completed *********************************")

logger.info("********************** Deleting sales data from local disk *********************")
delete_local_file(config.local_directory)
logger.info("********************** Deleting sales data from local disk Done *********************")

logger.info("********************** Deleting error files from local disk *********************")
delete_local_file(config.error_folder_path_local)
logger.info("********************** Deleting error files from local disk Done *********************")

logger.info("********************** Deleting customer data mart from local disk *********************")
delete_local_file(config.customer_data_mart_local_file)
logger.info("********************** Deleting customer data mart from local disk Done *********************")

logger.info("********************** Deleting sales team data mart from local disk *********************")
delete_local_file(config.sales_team_data_mart_local_file)
logger.info("********************** Deleting sales team data mart from local disk Done *********************")

logger.info("********************** Deleting partitioned sales team data mart from local disk *********************")
delete_local_file(config.sales_team_data_mart_partitioned_local_file)
logger.info("********************** Deleting partitioned sales team data mart from local disk Done *********************")


# Update the Status of Staging Table
# Get the current date and time
current_date = datetime.now()

# Format the current date and time to 'YYYY-MM-DD HH:MM:SS'
formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")

# Update the Status of Staging Table
update_statements = []
if correct_schema:
    for file in correct_schema:
        file_name = os.path.basename(file)

        # Update the query with the correctly formatted date enclosed in quotes
        statements = (
            f"UPDATE {db_name}.{config.product_stage_table} SET status = 'I', Updated_date = '{formatted_date}' "
            f"WHERE file_name = '{file_name}'")

        update_statements.append(statements)
        logger.info(f"Update statement created for staging table: {statements}")

    logger.info("************************* Connected with mySQL server *******************************************")

    # Establish MySQL connection (you'll need to implement this function or adjust accordingly)
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("************************* Connected with MySQL server successfully *******************************************")

    # Execute each update statement
    for statement in update_statements:
        cursor.execute(statement)
        connection.commit()
        logger.info(f"Update statement executed: {statement}")

    cursor.close()
    connection.close()

else:
    logger.error(
        "************************* There is some error in process in between *******************************************")
    sys.exit()

# Prompt to terminate the process
input("Press Enter to terminate the process")
















































































































#Example to read file from s3
# def read_file_from_s3(bucket_name, key):
#     try:
#         obj = s3_client.get_object(Bucket=bucket_name, Key=key)
#         file_content = obj['Body'].read().decode('utf-8')
#         return file_content
#     except Exception as e:
#         logger.error(f"Error reading file from S3: {e}")
#         return None
#
# #Example to upload file to s3
# def upload_file_to_s3(bucket_name, key, file_path):
#     try:
#         s3_client.upload_file(file_path, bucket_name, key)
#         logger.info(f"File '{file_path}' uploaded to '{bucket_name}/{key}' successfully.")
#     except Exception as e:
#          logger.error(f"Error uploading file to S3: {e}")
#
# if name == 'main':
#     # Example usage
#     bucket_name = "your-bucket-name"
#     key = "path/to/your/file.txt"
#     file_path = "local/path/to/your/file.txt"
#
#     # Read file from S3
#     content = read_file_from_s3(bucket_name, key)
#     if content:
#         print(f"File content:\n{content}")
#
#     # Upload file to S3
#     upload_file_to_s3(bucket_name, "uploaded_file.txt", file_path)



