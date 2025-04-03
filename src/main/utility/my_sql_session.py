import mysql.connector
from mysql.connector import Error
from src.main.utility.encrypt_decrypt import decrypt
from resources.dev import config# Ensure you import your config module

# Establish a connection
#print(f"password :{config.password})")
def get_mysql_connection():
    connection = None
    try:
        connection = mysql.connector.connect(
            host=config.host,
            user=config.user,
            password=decrypt(config.password),
            database=config.database  # Optional: specify if connecting to a database
        )
        if connection.is_connected():
            print("Connected to MySQL successfully")
            return connection
    except Error as err:
        print(f"Error: {err}")
        return None
    finally:
        if connection and not connection.is_connected():
            connection.close()
            print("Connection closed due to an error")
















# connection = mysql.connector.connect(
#     host="localhost",
#     user="root",
#     password="password",
#     database="manish"
# )
#
# # Check if the connection is successful
# if connection.is_connected():
#     print("Connected to MySQL database")
#
# cursor = connection.cursor()
#
# # Execute a SQL query
# query = "SELECT * FROM Shreyansh.testing"
# cursor.execute(query)
#
# # Fetch and print the results
# for row in cursor.fetchall():
#     print(row)
#
# # Close the cursor
# cursor.close()
#
# connection.close()
