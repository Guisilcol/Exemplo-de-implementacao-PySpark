#%% Importing the libraries
from os import getenv as env
import logging

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

from internal.mssql_handler import merge_into_mssql
from internal.pyspark_helper import get_pre_configured_spark_session_builder, get_jdbc_options

#%% Initialize the SparkSession
SPARK = get_pre_configured_spark_session_builder() \
    .appName("Load Notas Fiscais Table") \
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

def compute_condicao_pagamento_table(spark: SparkSession):
    """Compute the dataframe for the CONDICAO_PAGAMENTO table."""
    
    TABLE_NAME = 'CONDICAO_PAGAMENTO'
    df = spark.read \
        .format("jdbc") \
        .options(**get_jdbc_options()) \
        .option("dbtable", TABLE_NAME) \
        .load()
    
    return df

def compute_notas_fiscais_entrada_table(df_compras_stg: DataFrame, df_fornecedores: DataFrame, df_condicao_pagamento: DataFrame):
    """Compute the dataframe for the NOTAS_FISCAIS_ENTRADA table."""
    
    df = df_compras_stg.select(
        'NUMERO_NF',
        'DATA_EMISSAO',
        'VALOR_NET',
        'VALOR_TRIBUTO',
        'VALOR_TOTAL',
        'NOME_ITEM',
        'QTD_ITEM',
        'CONDICAO_PAGAMENTO',
        'CNPJ_FORNECEDOR'
    )
    
    df = df.join(df_fornecedores, df['CNPJ_FORNECEDOR'] == df_fornecedores['CNPJ'], 'inner')
    df = df.join(df_condicao_pagamento, df['CONDICAO_PAGAMENTO'] == df_condicao_pagamento['DESCRICAO'], 'LEFT')    
    
    df = df.select(
        df_compras_stg['NUMERO_NF'],
        df_fornecedores['ID_FORNECEDOR'],
        df_condicao_pagamento['ID_CONDICAO'],
        df_compras_stg['DATA_EMISSAO'],
        df_compras_stg['VALOR_NET'],
        df_compras_stg['VALOR_TRIBUTO'],
        df_compras_stg['VALOR_TOTAL'],
        df_compras_stg['NOME_ITEM'],
        df_compras_stg['QTD_ITEM']
    )
    
    df = df.withColumn('DATA_EMISSAO', F.to_date(F.col('DATA_EMISSAO'), 'yyyy-MM-dd'))
    df = df.withColumn('VALOR_NET', F.col('VALOR_NET').cast('decimal'))
    df = df.withColumn('VALOR_TRIBUTO', F.col('VALOR_TRIBUTO').cast('decimal'))
    df = df.withColumn('VALOR_TOTAL', F.col('VALOR_TOTAL').cast('decimal'))
    df = df.withColumn('QTD_ITEM', F.col('QTD_ITEM').cast('integer'))
    
    return df

#%% Job execution
if __name__ == "__main__":
    df_compras_stg = compute_compras_stg(SPARK)
    df_fornecedores = compute_fornecedores_table(SPARK)
    df_condicao_pagamento = compute_condicao_pagamento_table(SPARK)
    
    df = compute_notas_fiscais_entrada_table(df_compras_stg, df_fornecedores, df_condicao_pagamento)

    statistics = merge_into_mssql(df, 'NOTAS_FISCAIS_ENTRADA', ['NUMERO_NF'])

    LOGGER.info(f"Inserted {statistics['INSERT']} rows")
    LOGGER.info(f"Updated {statistics['UPDATE']} rows")


