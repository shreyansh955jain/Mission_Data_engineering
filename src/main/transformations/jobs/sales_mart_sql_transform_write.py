from pyspark.sql.functions import *
from pyspark.sql.window import Window
from resources.dev import config
from src.main.write.database_write import DatabaseWriter
import logging

# Initialize logger
logger = logging.getLogger(__name__)

def sales_mart_calculation_table_write(final_sales_team_data_mart_df):
    logger.info("Starting the sales mart calculation...")

    # Step 1: Prepare the sales data
    logger.info("Preparing the sales data...")

    # Check if 'sales_date' column exists and handle any missing column cases
    if "sales_date" not in final_sales_team_data_mart_df.columns:
        logger.error("Column 'sales_date' is missing in the DataFrame")
        return  # Exit the function if the column is missing

    # Define the window specification
    window = Window.partitionBy("store_id", "sales_person_id", "sales_month")

    final_sales_team_data_mart = final_sales_team_data_mart_df \
        .withColumn(
            "sales_month",
            concat(date_format(col("sales_date"), "yyyy-MM"), lit("-01"))  # Convert to 'YYYY-MM-01'
        ) \
        .withColumn("total_sales_every_month", sum("total_cost").over(window)) \
        .select(
            "store_id",
            "sales_person_id",
            concat(col("sales_person_first_name"), lit(" "), col("sales_person_last_name")).alias("full_name"),  # Use correct column names
            "sales_month",  # Match the column name "sales_month"
            "total_sales_every_month"
        ) \
        .distinct()

    logger.info(f"Sales data prepared with {final_sales_team_data_mart.count()} records.")

    # Step 2: Define the ranking window and calculate rank and incentive
    logger.info("Calculating rank and incentive...")

    rank_window = Window.partitionBy("store_id", "sales_person_id", "sales_month").orderBy(col("total_sales_every_month").desc())

    final_sales_team_data_mart_table = final_sales_team_data_mart \
        .withColumn("rank", row_number().over(rank_window)) \
        .withColumn(
            "incentive",
            when(col("rank") == 1, col("total_sales_every_month") * 0.1).otherwise(0)
        ) \
        .withColumn("incentive", round(col("incentive"), 2)) \
        .withColumn("total_sales", col("total_sales_every_month")) \
        .select(
            "store_id", "sales_person_id", "full_name",  # Correct column name "full_name"
            "sales_month", "total_sales", "incentive"
        )

    logger.info(f"Rank and incentive calculated for {final_sales_team_data_mart_table.count()} records.")

    # Step 3: Write the data into MySQL sales_team_data_mart table
    logger.info("Writing the Data into sales_team data_mart table...")

    db_writer = DatabaseWriter(url=config.url, properties=config.properties)
    db_writer.write_dataframe(final_sales_team_data_mart_table, config.sales_team_data_mart_table)

    logger.info("Data successfully written into sales_team data_mart table.")
