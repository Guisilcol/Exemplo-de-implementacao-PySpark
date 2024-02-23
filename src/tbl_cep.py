#%% Importing the libraries
from os import getenv as env
import logging

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

from internal.mssql_handler import merge_into_mssql
from internal.pyspark_helper import get_pre_configured_spark_session_builder

#%% Initialize the SparkSession
SPARK = get_pre_configured_spark_session_builder() \
    .appName("Load CEP Table") \
    .getOrCreate()

logging.basicConfig()
LOGGER = logging.getLogger("pyspark")
LOGGER.setLevel(logging.INFO)

#%% Load the function to compute the stage data for the file 'cep'
def compute_cep_table(spark: SparkSession):
    FILEPATH = f'{env("SOURCE_PATH")}/TB_CEP_BR_2018.csv'
    SCHEMA = T.StructType([
        T.StructField('CEP', T.IntegerType(), True),
        T.StructField('UF', T.StringType(), True),
        T.StructField('CIDADE', T.StringType(), True),
        T.StructField('BAIRRO', T.StringType(), True),
        T.StructField('LOGRADOURO', T.StringType(), True),
    ])

    df = spark.read.csv(FILEPATH, header=False, sep=';', schema=SCHEMA)
    
    df = df.withColumn('UF', F.trim(F.upper(F.col('UF'))))
    df = df.withColumn('CIDADE', F.trim(F.upper(F.col('CIDADE'))))
    df = df.withColumn('BAIRRO', F.trim(F.upper(F.col('BAIRRO'))))
    df = df.withColumn('LOGRADOURO', F.trim(F.upper(F.col('LOGRADOURO'))))
    
    return df
#%% Job execution
if __name__ == "__main__":
    df = compute_cep_table(SPARK)
    statiscs = merge_into_mssql(df, table_name='CEP', unique_keys=['CEP'])

    LOGGER.info(f"Inserted {statiscs['INSERT']} rows")
    LOGGER.info(f"Updated {statiscs['UPDATE']} rows")