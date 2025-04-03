import os
import findspark

# Ensure Spark binds to the correct IP (Update if needed)
os.environ["SPARK_LOCAL_IP"] = "192.168.1.10"

findspark.init()
from pyspark.sql import SparkSession
from src.main.utility.logging_config import logger

def spark_session():
    spark = SparkSession.builder \
        .appName("pyspark_DE_Project") \
        .config("spark.driver.extraClassPath", "/home/shreyansh-jain/mysql_jars/mysql-connector-java-8.0.26.jar") \
        .config("spark.executor.extraLibraryPath", "/home/shreyansh-jain/hadoop-3.4.0/lib/native") \
        .config("spark.driver.extraLibraryPath", "/home/shreyansh-jain/hadoop-3.4.0/lib/native") \
        .getOrCreate()

    print(spark.version)
    logger.info("Spark session started successfully: %s", spark)
    return spark




















# import os
# os.environ["SPARK_LOCAL_IP"] = "192.168.1.10"
# import findspark
# findspark.init()
# from pyspark.sql import SparkSession
# from src.main.utility.logging_config import logger
#
# def spark_session():
#     spark = SparkSession.builder \
#     .appName("pyspark_DE_Project") \
#     .config("spark.driver.extraClassPath", "/home/shreyansh-jain/mysql_jars/mysql-connector-java-8.0.26.jar") \
#     .config("spark.executor.extraLibraryPath", "/home/shreyansh-jain/hadoop-3.4.0/lib/native")\
#     .config("spark.driver.extraLibraryPath", "/home/shreyansh-jain/hadoop-3.4.0/lib/native")\
#     .getOrCreate()
#
#     print(spark.version)
#     logger.info("spark session %s", spark)
#     return spark
