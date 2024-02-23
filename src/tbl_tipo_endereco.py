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
    .appName("Load Tipo Endereco Table") \
    .getOrCreate()

logging.basicConfig()
LOGGER = logging.getLogger("pyspark")
LOGGER.setLevel(logging.INFO)

#%% Load the function to compute the stage data for the table 'tipo_pagamento'
def compute_tipo_pagamento(spark: SparkSession):
    TIPO_ENDERECO_FILEPATH = f'{env("SOURCE_PATH")}/tipo_endereco.csv'

    TIPO_ENDERECO_SCHEMA = T.StructType([
        T.StructField("id_tipo_endereco", T.IntegerType(), True),
        T.StructField("nome_tipo_endereco", T.StringType(), True),
        T.StructField("sigla_endereco", T.StringType(), True)
    ])
    
    df = spark.read.csv(TIPO_ENDERECO_FILEPATH, header=True, sep=',', schema=TIPO_ENDERECO_SCHEMA)
    
    # Data cleaning
    df = df.withColumn('nome_tipo_endereco', F.upper(F.col('nome_tipo_endereco')))
    df = df.withColumn('nome_tipo_endereco', F.trim(F.col('nome_tipo_endereco')))
    
    df = df.withColumn('sigla_endereco', F.upper(F.col('sigla_endereco')))
    df = df.withColumn('sigla_endereco', F.trim(F.col('sigla_endereco')))
    
    
    # Column renaming
    df = df.withColumnRenamed('id_tipo_endereco', 'ID_TIPO_ENDERECO')
    df = df.withColumnRenamed('nome_tipo_endereco', 'DESCRICAO')
    df = df.withColumnRenamed('sigla_endereco', 'SIGLA')
    
    return df
#%% Job execution
if __name__ == "__main__":
    df = compute_tipo_pagamento(SPARK)
    statiscs = merge_into_mssql(df, 'TIPO_ENDERECO', ['ID_TIPO_ENDERECO'])
    
    LOGGER.info(f"Inserted {statiscs['INSERT']} rows")
    LOGGER.info(f"Updated {statiscs['UPDATE']} rows")
    