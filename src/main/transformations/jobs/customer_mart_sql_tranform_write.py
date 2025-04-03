from pyspark.sql.functions import *
from pyspark.sql.window import Window
from resources.dev import config
from src.main.write.database_write import DatabaseWriter

# Calculation for customer mart
# Find out the customer total purchase every month
# Write the data into MySQL table
def customer_mart_calculation_table_write(final_customer_data_mart_df):
    # Define window specification
    window = Window.partitionBy("customer_id", "sales_date_month")

    # Prepare the final DataFrame
    final_customer_data_mart = final_customer_data_mart_df.withColumn(
        "sales_date_month",
        concat(date_format(col("sales_date"), "yyyy-MM"), lit("-01"))  # Convert to 'YYYY-MM-01'
    ).withColumn(
        "total_sales_every_month_by_each_customer",
        sum("total_cost").over(window)  # Sum the total cost for each customer and month
    ).select(
        "customer_id",
        concat(
            coalesce(col("first_name"), lit("")),
            lit(" "),
            coalesce(col("last_name"), lit(""))
        ).alias("full_name"),  # Concatenate first and last names
        "address",
        "phone_number",
        "sales_date_month",
        col("total_sales_every_month_by_each_customer").alias("total_sales")  # Rename column
    ).distinct()

    # Show the transformed DataFrame (Optional: for debugging)
    final_customer_data_mart.show()

    # Write the Data into MySQL customers_data_mart table
    try:
        db_writer = DatabaseWriter(url=config.url, properties=config.properties)
        db_writer.write_dataframe(final_customer_data_mart, config.customer_data_mart_table)
    except Exception as e:
        print(f"Error occurred while writing to MySQL: {e}")
