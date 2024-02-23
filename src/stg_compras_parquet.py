#%% Importing the libraries
from os import getenv as env
import logging

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T
from validate_docbr import CNPJ

from internal.pyspark_helper import get_jdbc_options, get_pre_configured_spark_session_builder

#%% Initialize the SparkSession
SPARK = get_pre_configured_spark_session_builder() \
    .appName("Load Staging Data") \
    .getOrCreate()

logging.basicConfig()
LOGGER = logging.getLogger("pyspark")
LOGGER.setLevel(logging.INFO)

#%% Load the function to compute dataframes
def compute_cep(spark: SparkSession):
    """Compute the dataframe for the fornecedores table."""
    TABLE_NAME = 'CEP'
    return spark.read \
        .format("jdbc") \
        .options(**get_jdbc_options()) \
        .option("dbtable", TABLE_NAME) \
        .load()
        
def compute_compras_stg(spark: SparkSession, df_ceps: DataFrame):
    """Compute the stage data for the table 'compras' and return a DataFrame with the stage data and a rejected DataFrame with the rejected data and the inconsistency type."""
    COMPRAS_FILEPATH = f'{env("SOURCE_PATH")}/compras.csv'
    
    def subtract_by_index(df: DataFrame, df_to_subtract: DataFrame) -> DataFrame:
        """Subtract the rows of a DataFrame by the index column."""
        return df.join(df_to_subtract, df['index'] == df_to_subtract['index'], 'left_anti').select(df.columns)
    
    DF = spark.read.csv(COMPRAS_FILEPATH, header=True)
    df = DF
        
    # Trim all columns 
    for column in df.columns:
        df = df.withColumn(column, F.trim(F.col(column)))
        
    # Add a temp index column
    df = df.withColumn('index', F.monotonically_increasing_id())
    
    # NOME_FORNECEDOR column treatment
    df = df.withColumn('NOME_FORNECEDOR', F.upper(F.col('NOME_FORNECEDOR')))
    df = df.withColumn('NOME_FORNECEDOR', F.trim(F.col('NOME_FORNECEDOR')))
    
    # CONDICAO_PAGAMENTO column treatment
    df = df.withColumn('CONDICAO_PAGAMENTO', F.upper(F.col('CONDICAO_PAGAMENTO')))
    df = df.withColumn('CONDICAO_PAGAMENTO', F.trim(F.col('CONDICAO_PAGAMENTO')))
    
    df = df.withColumn('CONDICAO_PAGAMENTO',
        F.when(
                    F.col('CONDICAO_PAGAMENTO') == 'ENTRADA/30/60/90 DIAS', '30/60/90 DIAS')
            .when(  F.col('CONDICAO_PAGAMENTO') == 'A VISTA',               'A VISTA')
            .when(  F.col('CONDICAO_PAGAMENTO') == '30 DIAS',               '30 DIAS')
            .when(  F.col('CONDICAO_PAGAMENTO') == '30/60 DIAS',            '30/60 DIAS')
            .when(  F.col('CONDICAO_PAGAMENTO') == 'ENTRADA/30 DIAS',       'ENTRADA/30 DIAS')
            .when(  F.col('CONDICAO_PAGAMENTO') == 'ENTRADA/30/60 DIAS',    'ENTRADA/30/60 DIAS')
        .otherwise( F.col('CONDICAO_PAGAMENTO') + ' (invalid)')
    )
    
    # COMPLEMENTO column treatment
    df = df.na.fill('N/A', subset=['COMPLEMENTO'])

    # Compute a DataFrame with the nulls 
    df_not_null = df.dropna(how='any', subset=[column for column in df.columns if column != 'COMPLEMENTO'])
    
    df_nulls = (
        subtract_by_index(df, df_not_null)
        .withColumn('inconsistency', F.lit('null column(s)'))
    )
    
    # Compute a DataFrame with the invalid payment condition
    df_invalid_payment_condition = (
        df
        .filter(F.col('CONDICAO_PAGAMENTO').contains('(invalid)'))
        .withColumn('inconsistency', F.lit('invalid payment condition'))
    )
    
    # Compute a DataFrame with the invalid CEP
    df_ceps = df_ceps.select('CEP')
    df_ceps = df_ceps.withColumnRenamed('CEP', 'CEP_VALIDO')
    
    df_invalid_ceps = (
        # Lookup the valid CEPs and filter the valid ones
        df 
        .join(df_ceps, 
              df['CEP'].cast('int') == df_ceps['CEP_VALIDO'].cast('int'), 
              'left'
        )
        .filter(F.col('CEP_VALIDO').isNull())
        .select(df.columns)
        .withColumn('inconsistency', F.lit('invalid CEP'))
    )
    
    # Compute a DataFrame with the invalid CNPJ
    @F.udf(T.BooleanType())
    def udf_is_cnpj_valid(cnpj: str) -> bool:
        if cnpj is None:
            return False
        return CNPJ().validate(cnpj)
    
    df = df.cache()
    df_invalids_cnpj = df.filter(~udf_is_cnpj_valid(F.col('CNPJ_FORNECEDOR')))
    df_invalids_cnpj = df_invalids_cnpj.withColumn('inconsistency', F.lit('invalid CNPJ'))
    
    # Remove the duplicates AND create a Dataframe with duplicated rows
    df_duplicated = (
        df
        .groupBy(DF.columns)
        .count()
        .where(F.col('count') > 1)
        .drop('count')
        .withColumn('inconsistency', F.lit('duplicated'))
    )
    
    # Remove all rejeted data by the index column
    df = (
        df
        .transform(lambda df: subtract_by_index(df, df_nulls))
        .transform(lambda df: subtract_by_index(df, df_invalid_payment_condition))
        .transform(lambda df: subtract_by_index(df, df_invalids_cnpj))
        .transform(lambda df: subtract_by_index(df, df_invalid_ceps))
        .drop('index').dropDuplicates()
    )
    
    df_rejected = (
        df_duplicated.drop('index')
        .union(df_nulls.drop('index'))
        .union(df_invalid_payment_condition.drop('index'))
        .union(df_invalids_cnpj.drop('index'))
        .union(df_invalid_ceps.drop('index'))
        .dropDuplicates()
        .groupBy(df.columns)
        .agg(F.concat_ws(", ", F.collect_list(F.col('inconsistency'))).alias('inconsistencies'))
    )

    
    return df, df_rejected

def load_compras_stg(df: DataFrame):
    """Load the stage data to the stage parquet file."""
    # Considering that the files generated by the processes must be easy to understand (as they will be available on GitHub) 
    # I chose to generate the data using Pandas. However, if you are interested, the code commented below loads the file using Spark
    #filepath = f'{env("TARGET_PATH")}/compras_stg'
    #df.write.parquet(filepath, mode='overwrite')
    
    import pandas as pd
    filepath = f'{env("TARGET_PATH")}/compras_stg.parquet'
    pd.DataFrame(df.toPandas()).to_parquet(filepath)
    
def load_rejected_compras_stg(df: DataFrame):
    """Load the rejected data to the rejected csv file."""
    
    # Considering that the files generated by the processes must be easy to understand (as they will be available on GitHub) 
    # I chose to generate the data using Pandas. However, if you are interested, the code commented below loads the file using Spark
    #filepath = f'{env("TARGET_PATH")}/rejected_compras_stg'
    #df.write.csv(filepath, mode='overwrite', header=True)
    
    import pandas as pd
    filepath = f'{env("TARGET_PATH")}/rejected_compras_stg.csv'
    pd.DataFrame(df.toPandas()).to_csv(filepath, index=False, sep=';')
#%% Job execution
if __name__ == "__main__":
    df_ceps = compute_cep(SPARK)
    df, df_rejected = compute_compras_stg(SPARK, df_ceps)
    
    load_compras_stg(df)
    load_rejected_compras_stg(df_rejected)
    
    written_lines = df.count()
    rejected_lines = df_rejected.count()
    
    LOGGER.info(f"Written {written_lines} rows")
    LOGGER.info(f"Rejected {rejected_lines} rows")
    
