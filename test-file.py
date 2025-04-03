import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"


from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.master("local[*]").appName("PySparkTest").getOrCreate()

# Create a DataFrame
data = [("Shreyansh", 25), ("Krishna", 30), ("Manav", 28)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Show DataFrame
df.show()

# Stop SparkSession
spark.stop()
