#%% Importing the libraries
from os import getenv as env

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from external_libs.pyspark_helper import get_pre_configured_glue_session
from external_libs.data_loader import merge_dataframe_with_iceberg_table

#%% Initialize the SparkSession
SPARK, SPARK_CONTEXT, GLUE_CONTEXT, JOB, ARGS = get_pre_configured_glue_session({
    "SOURCE_PATH": env("SOURCE_PATH"),
    "AWS_WAREHOUSE": env("AWS_WAREHOUSE")
})

#%% Load the function to compute the stage data for the table 'tipo_pagamento'
def compute_tipo_pagamento(spark: SparkSession, source_path: str = None):
    TIPO_ENDERECO_FILEPATH = f'{source_path}/tipo_endereco.csv'

    df = spark.read.csv(TIPO_ENDERECO_FILEPATH, header=True, sep=',')
    
    # Data cleaning
    df = df.withColumn('nome_tipo_endereco', F.upper(F.col('nome_tipo_endereco')))
    df = df.withColumn('nome_tipo_endereco', F.trim(F.col('nome_tipo_endereco')))
    
    df = df.withColumn('sigla_endereco', F.upper(F.col('sigla_endereco')))
    df = df.withColumn('sigla_endereco', F.trim(F.col('sigla_endereco')))
    
    
    # Column renaming
    df = df.withColumnRenamed('id_tipo_endereco', 'ID_TIPO_ENDERECO')
    df = df.withColumnRenamed('nome_tipo_endereco', 'DESCRICAO')
    df = df.withColumnRenamed('sigla_endereco', 'SIGLA')
    
    df = df.withColumn('ID_TIPO_ENDERECO', F.col('ID_TIPO_ENDERECO').cast('int'))
    return df
#%% Job execution
if __name__ == "__main__":
    df = compute_tipo_pagamento(SPARK, ARGS['SOURCE_PATH'])
    merge_dataframe_with_iceberg_table(GLUE_CONTEXT, df, 'compras', 'tipo_endereco', ['ID_TIPO_ENDERECO'])
    