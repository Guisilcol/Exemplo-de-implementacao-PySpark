#%% Importing the libraries
from os import getenv as env

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T
from awsglue.context import GlueContext

from external_libs.pyspark_helper import get_pre_configured_glue_session
from external_libs.data_loader import merge_dataframe_with_iceberg_table

#%% Initialize the SparkSession
SPARK, SPARK_CONTEXT, GLUE_CONTEXT, JOB, ARGS = get_pre_configured_glue_session({
    "WORK_PATH": env("WORK_PATH"),
    "AWS_WAREHOUSE": env("AWS_WAREHOUSE")
})

#%% Load the function to compute dataframes
def compute_compras_stg(spark: SparkSession, source_path: str = None):
    """Compute the dataframe for the compras_stg table."""
    
    COMPRAS_STG_FILEPATH = f'{source_path}/compras.parquet'
    return spark.read.parquet(COMPRAS_STG_FILEPATH)
    
def compute_fornecedores_table(glue_context: GlueContext):
    """Compute the dataframe for the fornecedores table."""
    return glue_context.create_data_frame_from_catalog(database = "compras", table_name = 'FORNECEDORES')

def compute_tipo_endereco_table(glue_context: GlueContext):
    """Compute the dataframe for the tipo_endereco table."""
    return glue_context.create_data_frame_from_catalog(database = "compras", table_name = 'TIPO_ENDERECO')

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
    
    df = df.withColumn('CEP', F.col('CEP').cast(T.IntegerType()))
    df = df.withColumn('ID_ENDERECO_FORNECEDOR', F.hash(F.concat_ws('', df['CEP'], df['ID_FORNECEDOR'], df['ID_TIPO_ENDERECO'])))
    
    return df

#%% Job execution
if __name__ == "__main__":
    df_compras_stg = compute_compras_stg(SPARK, ARGS['WORK_PATH'])
    df_fornecedores = compute_fornecedores_table(GLUE_CONTEXT)
    df_tipo_endereco = compute_tipo_endereco_table(GLUE_CONTEXT)
    
    df = compute_enderecos_fornecedores_table(df_compras_stg, df_fornecedores, df_tipo_endereco)
    
    merge_dataframe_with_iceberg_table(GLUE_CONTEXT, df, 'compras', 'enderecos_fornecedores', ['CEP', 'ID_FORNECEDOR', 'ID_TIPO_ENDERECO'])