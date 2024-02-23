from os import getenv as env

from pyspark.sql import SparkSession

def get_pre_configured_spark_session_builder():
    """Return a pre-configured SparkSession with the necessary configurations to connect to Master node and to the SQL Server database."""
    
    return SparkSession.builder \
        .config('spark.driver.extraClassPath', env('MSSQL_JDBC')) \
        .config('spark.executor.extraClassPath', env('MSSQL_JDBC')) \
        .master(env('SPARK_MASTER'))
        
        
def get_jdbc_options():
    """Return a dictionary with the necessary configurations to connect to the SQL Server database."""
    
    return {
        "url": f"""jdbc:sqlserver://{env('DB_SERVER_NAME')}:1433;databaseName={env('DB_NAME')};encrypt=false""",
        "user": env("DB_USER"),
        "password": env("DB_PASSWORD"),
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }