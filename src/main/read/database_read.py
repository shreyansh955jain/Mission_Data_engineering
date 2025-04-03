from src.main.utility.encrypt_decrypt import decrypt

class DatabaseReader:
    def __init__(self, url, properties):
        self.url = url
        self.properties = properties
        # Decrypt the password before using it
        self.properties["password"] = decrypt(self.properties["password"])

    def create_dataframe(self, spark, table_name):
        df = spark.read.jdbc(
            url=self.url,
            table=table_name,
            properties=self.properties
        )
        return df
