#%% Importing the libraries
from os import getenv as env

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

from external_libs.pyspark_helper import get_pre_configured_glue_session
from external_libs.data_loader import merge_dataframe_with_iceberg_table

#%% Initialize the SparkSession
SPARK, SPARK_CONTEXT, GLUE_CONTEXT, JOB, ARGS = get_pre_configured_glue_session({
    "SOURCE_PATH": env("SOURCE_PATH"),
    "AWS_WAREHOUSE": env("AWS_WAREHOUSE")
})


#%% Load compute functions 
def compute_condicao_pagamento(spark: SparkSession):
    FILEPATH = f'{env("SOURCE_PATH")}/condicao_pagamento.csv'

    df = spark.read.csv(FILEPATH, header=True, sep=',')
    df = df.select('ID_CONDICAO', 'DESCRICAO', 'QTD_PARCELAS')
    df = df.withColumn('DESCRICAO', F.upper(F.trim(F.col('DESCRICAO'))))
    
    df = df.withColumn('QTD_PARCELAS', F.col('QTD_PARCELAS').cast(T.IntegerType()))
    
    df = df.withColumn('ID_CONDICAO', F.col('ID_CONDICAO').cast(T.IntegerType()))
    
    return df
    

#%% Job execution
if __name__ == "__main__":
    df = compute_condicao_pagamento(SPARK)
    merge_dataframe_with_iceberg_table(GLUE_CONTEXT, df, 'compras', 'condicao_pagamento', ['ID_CONDICAO'])

