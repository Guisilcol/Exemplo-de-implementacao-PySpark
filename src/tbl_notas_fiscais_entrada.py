#%% Importing the libraries
from os import getenv as env

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from awsglue.context import GlueContext 

from external_libs.pyspark_helper import get_pre_configured_glue_session
from external_libs.data_loader import merge_dataframe_with_iceberg_table

#%% Initialize the SparkSession
SPARK, SPARK_CONTEXT, GLUE_CONTEXT, JOB, ARGS = get_pre_configured_glue_session({
    "WORK_PATH": env("WORK_PATH"),
    "AWS_WAREHOUSE": env("AWS_WAREHOUSE")
})

#%% Load the function to compute dataframes
def compute_compras_stg(spark: SparkSession, source_path: str):
    """Compute the dataframe for the compras_stg table."""

    COMPRAS_STG_FILEPATH = f'{source_path}/compras.parquet'
    return spark.read.parquet(COMPRAS_STG_FILEPATH)
    
def compute_fornecedores_table(glue_context: GlueContext):
    """Compute the dataframe for the fornecedores table."""
    return glue_context.create_data_frame_from_catalog(database = "compras", table_name = 'FORNECEDORES')

def compute_condicao_pagamento_table(glue_context: GlueContext):
    """Compute the dataframe for the CONDICAO_PAGAMENTO table."""
    return glue_context.create_data_frame_from_catalog(database = "compras", table_name = 'CONDICAO_PAGAMENTO')

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
    
    df = df.withColumn('ID_NF_ENTRADA', F.hash(df['NUMERO_NF']))
    
    return df

#%% Job execution
if __name__ == "__main__":
    df_compras_stg = compute_compras_stg(SPARK, ARGS['WORK_PATH'])
    df_fornecedores = compute_fornecedores_table(GLUE_CONTEXT)
    df_condicao_pagamento = compute_condicao_pagamento_table(GLUE_CONTEXT)
    
    df = compute_notas_fiscais_entrada_table(df_compras_stg, df_fornecedores, df_condicao_pagamento)

    merge_dataframe_with_iceberg_table(GLUE_CONTEXT, df, 'compras', 'notas_fiscais_entrada', ['NUMERO_NF'])

