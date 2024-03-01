#%% Importing the libraries
from os import getenv as env

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from external_libs.pyspark_helper import get_pre_configured_glue_session
from external_libs.data_loader import merge_dataframe_with_iceberg_table

#%% Initialize the SparkSession
SPARK, SPARK_CONTEXT, GLUE_CONTEXT, JOB, ARGS = get_pre_configured_glue_session({
    "WORK_PATH": env("WORK_PATH"),
    "AWS_WAREHOUSE": env("AWS_WAREHOUSE")
})

#%% Load the function to compute fornecedores dataframe
def compute_fornecedores(spark: SparkSession):
    COMPRAS_STG_FILEPATH = f'{env("WORK_PATH")}/compras.parquet'
    
    df = spark.read.format('parquet').load(COMPRAS_STG_FILEPATH)
    
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
       
    df = df.withColumn('ID_FORNECEDOR', F.hash(F.col('CNPJ')))   
       
    return df

#%% Job execution
if __name__ == "__main__":
    df = compute_fornecedores(SPARK)    
    merge_dataframe_with_iceberg_table(GLUE_CONTEXT, df, 'compras', 'fornecedores', ['CNPJ'])

