#%% Importing the libraries
import logging

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T

from internal.mssql_handler import merge_into_mssql
from internal.pyspark_helper import get_pre_configured_spark_session_builder, get_jdbc_options

#%% Initialize the SparkSession
SPARK = get_pre_configured_spark_session_builder() \
    .appName("Load Programacao Pagamento Table") \
    .getOrCreate()

logging.basicConfig()
LOGGER = logging.getLogger("pyspark")
LOGGER.setLevel(logging.INFO)

def compute_notas_fiscais_de_entrada(spark: SparkSession):
    """Compute the dataframe using a query"""
    SQL = """
    SELECT
        A.ID_NF_ENTRADA, 
        A.DATA_EMISSAO,
        A.VALOR_TOTAL,
        B.QTD_PARCELAS
    FROM
        NOTAS_FISCAIS_ENTRADA A
        INNER JOIN CONDICAO_PAGAMENTO B  
            ON A.ID_CONDICAO = B.ID_CONDICAO
    """
    
    return spark.read \
        .format("jdbc") \
        .options(**get_jdbc_options()) \
        .option("query", SQL) \
        .load()


#%% Load the function to compute the stage data for the table 'tipo_pagamento'
def compute_programacao_pagamento(df_notas_fiscais_entrada: DataFrame):
    """Compute the Programacao Pagamento dataframe using the Notas Fiscais de Entrada dataframe"""
    
    df = (
        df_notas_fiscais_entrada
        .withColumn("PARCELAS", F.sequence(F.lit(1), F.col("QTD_PARCELAS")))
        .withColumn("NUM_PARCELA", F.explode(F.col("PARCELAS")))
        .drop("PARCELAS")
    )
    
    df = df.withColumn("VALOR_PARCELA", F.col("VALOR_TOTAL") / F.col("QTD_PARCELAS"))
    # I using the expr function because the F.add_months function not accept a column as integer parameter to increment the date 
    df = df.withColumn("DATA_VENCIMENTO", F.expr("add_months(DATA_EMISSAO, NUM_PARCELA - 1)"))
    
    # All dates equal or minor than today are considered as "PAGO", else "PENDENTE"
    df = (
        df
        .withColumn("STATUS_PAGAMENTO",   
                    F.when(F.col("DATA_VENCIMENTO") <= F.current_date(), F.lit("PAGO"))
                    .otherwise(F.lit("PENDENTE")))
    )
    
    df = df.select(
        'ID_NF_ENTRADA',
        'DATA_VENCIMENTO',
        'NUM_PARCELA',
        'VALOR_PARCELA',
        'STATUS_PAGAMENTO'
    )
    
    return df
#%% Job execution
if __name__ == "__main__":
    df_notas_fiscais_entrada = compute_notas_fiscais_de_entrada(SPARK)
    df = compute_programacao_pagamento(df_notas_fiscais_entrada)
    
    statiscs = merge_into_mssql(df, 'PROGRAMACAO_PAGAMENTO', ['ID_NF_ENTRADA', 'NUM_PARCELA'])
    
    LOGGER.info(f"Inserted {statiscs['INSERT']} rows")
    LOGGER.info(f"Updated {statiscs['UPDATE']} rows")
    