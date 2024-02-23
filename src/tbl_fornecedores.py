#%% Importing the libraries
from os import getenv as env
import logging

from pyspark.sql import SparkSession

from internal.mssql_handler import merge_into_mssql
from internal.pyspark_helper import get_pre_configured_spark_session_builder

#%% Initialize the SparkSession
SPARK = get_pre_configured_spark_session_builder() \
    .appName("Load Fornecedores Table") \
    .getOrCreate()

logging.basicConfig()
LOGGER = logging.getLogger("pyspark")
LOGGER.setLevel(logging.INFO)

#%% Load the function to compute fornecedores dataframe
def compute_fornecedores(spark: SparkSession):
    COMPRAS_STG_FILEPATH = f'{env("TARGET_PATH")}/compras_stg.parquet'
    
    df = spark.read.parquet(COMPRAS_STG_FILEPATH)
    
    # Select the columns, drop duplicates and rename the columns
    df = df.select('NOME_FORNECEDOR', 'CNPJ_FORNECEDOR', 'EMAIL_FORNECEDOR', 'TELEFONE_FORNECEDOR')
    df = df.dropDuplicates()
    
    df = (
        df
        .withColumnRenamed('NOME_FORNECEDOR', 'NOME')
        .withColumnRenamed('CNPJ_FORNECEDOR', 'CNPJ')
        .withColumnRenamed('EMAIL_FORNECEDOR', 'EMAIL')
        .withColumnRenamed('TELEFONE_FORNECEDOR', 'TELEFONE')
    )
       
    return df

#%% Job execution
if __name__ == "__main__":
    df = compute_fornecedores(SPARK)    
    statistics = merge_into_mssql(df, 'FORNECEDORES', ['CNPJ'])

    LOGGER.info(f"Inserted {statistics['INSERT']} rows")
    LOGGER.info(f"Updated {statistics['UPDATE']} rows")


    
