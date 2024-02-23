#%% Importing the libraries
from os import getenv as env
import logging

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

from internal.mssql_handler import merge_into_mssql
from internal.pyspark_helper import get_pre_configured_spark_session_builder, get_jdbc_options

#%% Initialize the SparkSession
SPARK = get_pre_configured_spark_session_builder() \
    .appName("Load Endereco Fornecedores Table") \
    .getOrCreate()

logging.basicConfig()
LOGGER = logging.getLogger("pyspark")
LOGGER.setLevel(logging.INFO)

#%% Load the function to compute dataframes
def compute_compras_stg(spark: SparkSession):
    """Compute the dataframe for the compras_stg table."""
    COMPRAS_STG_FILEPATH = f'{env("TARGET_PATH")}/compras_stg.parquet'
    return spark.read.parquet(COMPRAS_STG_FILEPATH)
    
def compute_fornecedores_table(spark: SparkSession):
    """Compute the dataframe for the fornecedores table."""
    TABLE_NAME = 'FORNECEDORES'
    return spark.read \
        .format("jdbc") \
        .options(**get_jdbc_options()) \
        .option("dbtable", TABLE_NAME) \
        .load()

def compute_tipo_endereco_table(spark: SparkSession):
    """Compute the dataframe for the tipo_endereco table."""
    TABLE_NAME = 'TIPO_ENDERECO'
    df = spark.read \
        .format("jdbc") \
        .options(**get_jdbc_options()) \
        .option("dbtable", TABLE_NAME) \
        .load()
    
    return df

def compute_enderecos_fornecedores_table(df_compras_stg: DataFrame, df_fornecedores: DataFrame, df_tipo_endereco: DataFrame):
    """Compute the dataframe for the enderecos_fornecedores table."""
    
    df = df_compras_stg.select(
            'CNPJ_FORNECEDOR',
            'TIPO_ENDERECO',
            'NUM_ENDERECO',
            'COMPLEMENTO',
            'CEP'
        )
    
    df = df.dropDuplicates()

    df = df.join(df_fornecedores, df['CNPJ_FORNECEDOR'] == df_fornecedores['CNPJ'], 'inner')
    df = df.join(df_tipo_endereco, F.upper(df['TIPO_ENDERECO']) == df_tipo_endereco['DESCRICAO'], 'inner')
    
    df = df.select(
        df_compras_stg['CEP'],
        df_fornecedores['ID_FORNECEDOR'],
        df_tipo_endereco['ID_TIPO_ENDERECO'],
        df_compras_stg['NUM_ENDERECO'],   
        df_compras_stg['COMPLEMENTO']
    )
    
    return df

#%% Job execution
if __name__ == "__main__":
    df_compras_stg = compute_compras_stg(SPARK)
    df_fornecedores = compute_fornecedores_table(SPARK)
    df_tipo_endereco = compute_tipo_endereco_table(SPARK)
    
    df = compute_enderecos_fornecedores_table(df_compras_stg, df_fornecedores, df_tipo_endereco)
    
    statiscs = merge_into_mssql(df, 'ENDERECOS_FORNECEDORES', ['CEP', 'ID_FORNECEDOR', 'ID_TIPO_ENDERECO'])
    
    LOGGER.info(f"Inserted {statiscs['INSERT']} rows")
    LOGGER.info(f"Updated {statiscs['UPDATE']} rows")


    
