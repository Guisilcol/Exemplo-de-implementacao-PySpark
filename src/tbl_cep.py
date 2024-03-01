#%% Importing the libraries
from os import getenv as env

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions    as F
import pyspark.sql.types as T

from external_libs.pyspark_helper import get_pre_configured_glue_session
from external_libs.data_loader import merge_dataframe_with_iceberg_table

#%% Initialize the SparkSession
SPARK, SPARK_CONTEXT, GLUE_CONTEXT, JOB, ARGS = get_pre_configured_glue_session({
    "SOURCE_PATH": env("SOURCE_PATH"),
    "AWS_WAREHOUSE": env("AWS_WAREHOUSE")
})

#%% Load the function to compute dataframes
def compute_cep_table(spark: SparkSession, source_path: str) -> DataFrame:
    FILEPATH = f'{source_path}/TB_CEP_BR_2018.csv'
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
    df = compute_cep_table(GLUE_CONTEXT, ARGS['SOURCE_PATH'])
    merge_dataframe_with_iceberg_table(GLUE_CONTEXT, df, database='compras', table='cep', unique_keys=['CEP'])

