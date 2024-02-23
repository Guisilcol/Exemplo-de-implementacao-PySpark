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
    .appName("Load CONDICAO_PAGAMENTO Table") \
    .getOrCreate()

logging.basicConfig()
LOGGER = logging.getLogger("pyspark")
LOGGER.setLevel(logging.INFO)

#%% Load compute functions 
def compute_condicao_pagamento(spark: SparkSession):
    FILEPATH = f'{env("SOURCE_PATH")}/condicao_pagamento.csv'

    df = spark.read.csv(FILEPATH, header=True, sep=',')
    df = df.select('ID_CONDICAO', 'DESCRICAO', 'QTD_PARCELAS')
    df = df.withColumn('DESCRICAO', F.upper(F.trim(F.col('DESCRICAO'))))
    
    df = df.withColumn('QTD_PARCELAS', F.col('QTD_PARCELAS').cast(T.IntegerType()))
    
    return df
    

#%% Job execution
if __name__ == "__main__":
    df = compute_condicao_pagamento(SPARK)
    statiscs = merge_into_mssql(df, 'CONDICAO_PAGAMENTO', ['ID_CONDICAO'])
    
    LOGGER.info(f"Inserted {statiscs['INSERT']} rows")
    LOGGER.info(f"Updated {statiscs['UPDATE']} rows")

