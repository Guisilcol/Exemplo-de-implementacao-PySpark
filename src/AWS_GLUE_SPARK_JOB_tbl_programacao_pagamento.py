#%% Importing the libraries
from os import getenv as env

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from awsglue.context import GlueContext 

from external_libs.pyspark_helper import get_pre_configured_glue_session
from external_libs.data_loader import merge_dataframe_with_iceberg_table

#%% Initialize the SparkSession
SPARK, SPARK_CONTEXT, GLUE_CONTEXT, JOB, ARGS = get_pre_configured_glue_session({
    "AWS_WAREHOUSE": env("AWS_WAREHOUSE")
})

#%% Load the function to compute dataframes
def compute_notas_fiscais_de_entrada(glue_context: GlueContext):
    """Compute the dataframe using a query"""
    SQL = """
    SELECT
        A.ID_NF_ENTRADA, 
        A.DATA_EMISSAO,
        A.VALOR_TOTAL,
        B.QTD_PARCELAS
    FROM
        glue_catalog.compras.notas_fiscais_entrada A
        INNER JOIN glue_catalog.compras.condicao_pagamento B  
            ON A.ID_CONDICAO = B.ID_CONDICAO
    """
    
    return glue_context.sql(SQL)

def compute_programacao_pagamento_pendente(glue_context: GlueContext):
    """Compute the programcao_pagamento dataframe"""
    
    SQL = """
        SELECT ID_NF_ENTRADA, DATA_VENCIMENTO, NUM_PARCELA, VALOR_PARCELA 
        FROM glue_catalog.compras.programacao_pagamento 
        WHERE STATUS_PAGAMENTO = 'PENDENTE'
    """
    return glue_context.sql(SQL)

#%% Load the function to compute the stage data for the table 'tipo_pagamento'
def compute_programacao_pagamento(df_notas_fiscais_entrada: DataFrame, df_programacao_pagamento_pendente: DataFrame):
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
    
    df = df.select(
        'ID_NF_ENTRADA',
        'DATA_VENCIMENTO',
        'NUM_PARCELA',
        'VALOR_PARCELA'
    )
    
    df = df.union(df_programacao_pagamento_pendente)
    
    # All dates equal or minor than today are considered as "PAGO", else "PENDENTE"
    df = (
        df
        .withColumn("STATUS_PAGAMENTO",   
                    F.when(F.col("DATA_VENCIMENTO") <= F.current_date(), F.lit("PAGO"))
                    .otherwise(F.lit("PENDENTE")))
    )
    
    df = df.withColumn('ID_PROG_PAGAMENTO', F.hash(df['ID_NF_ENTRADA'], df['NUM_PARCELA']))
    
    return df
#%% Job execution
if __name__ == "__main__":
    df_notas_fiscais_entrada = compute_notas_fiscais_de_entrada(SPARK)
    df_programacao_pendente = compute_programacao_pagamento_pendente(SPARK)
    df = compute_programacao_pagamento(df_notas_fiscais_entrada, df_programacao_pendente)
    
    merge_dataframe_with_iceberg_table(GLUE_CONTEXT, df, 'compras', 'programacao_pagamento', ['ID_NF_ENTRADA', 'NUM_PARCELA'])
    