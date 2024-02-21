#%% Importing the libraries
from os import getenv as env
import logging

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T

from validate_docbr import CNPJ

#%% Initialize the SparkSession
SPARK = SparkSession.builder.appName("Load Staging Data").getOrCreate()

logging.basicConfig()
LOGGER = logging.getLogger("pyspark")
LOGGER.setLevel(logging.INFO)

#%% Load the function to compute the stage data for the table 'compras'
def compute_compras_stg(spark: SparkSession):
    """Compute the stage data for the table 'compras' and return a DataFrame with the stage data and a rejected DataFrame with the rejected data and the inconsistency type."""
    
    def subtract_by_index(df: DataFrame, df_to_subtract: DataFrame) -> DataFrame:
        """Subtract the rows of a DataFrame by the index column."""
        return df.join(df_to_subtract, df['index'] == df_to_subtract['index'], 'left_anti').select(df.columns)
    
    filepath = f'{env("SOURCE_PATH")}/compras.csv'
    
    DF = spark.read.csv(filepath, header=True)
        
    # Trim all columns 
    for column in DF.columns:
        df = DF.withColumn(column, F.trim(F.col(column)))
        
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
            .when(F.col('CONDICAO_PAGAMENTO') == 'A VISTA', 'A VISTA')
            .when(F.col('CONDICAO_PAGAMENTO') == '30 DIAS', '30 DIAS')
            .when(F.col('CONDICAO_PAGAMENTO') == 'ENTRADA/30 DIAS', '30 DIAS')
            .when(F.col('CONDICAO_PAGAMENTO') == '30/60 DIAS', '30/60 DIAS')
            .when(F.col('CONDICAO_PAGAMENTO') == 'ENTRADA/30/60 DIAS', '30/60 DIAS')
        .otherwise(F.col('CONDICAO_PAGAMENTO') + ' (invalid)')
    )
    
    # COMPLEMENTO column treatment
    df = df.na.fill('N/A', subset=['COMPLEMENTO'])

    # Compute a DataFrame with the nulls 
    _df_not_nulls = df.dropna(subset=[column for column in df.columns if column != 'COMPLEMENTO'])
    
    df_nulls = (
        df
        .transform(lambda df: subtract_by_index(df, _df_not_nulls))
        .withColumn('inconsistency', F.lit('null column(s)'))
    )
    
    # Compute a DataFrame with the invalid payment condition
    df_invalid_payment_condition = (
        df
        .filter(F.col('CONDICAO_PAGAMENTO').contains('(invalid)'))
        .withColumn('inconsistency', F.lit('invalid payment condition'))
    )
    
    # Compute a DataFrame with the invalid CEP
    DF_CEPS = spark.read.csv(f'{env("SOURCE_PATH")}/TB_CEP_BR_2018.csv', header=False, sep=';')
    df_ceps = DF_CEPS.withColumnRenamed('_c0', 'CEP_VALIDO')
    df_ceps = df_ceps.select('CEP_VALIDO')
    
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
    columns_without_index = df.columns
    df_duplicated = (
        df
        .groupBy(columns_without_index)
        .count()
        .where(F.col('count') > 1)
        .drop('count')
        .withColumn('inconsistency', F.lit('duplicated'))
    )
    
    # Remove all rejeted data by the index column, except for the duplicated rows
    df = (
        df
        .transform(lambda df: subtract_by_index(df, df_invalid_payment_condition))
        .transform(lambda df: subtract_by_index(df, df_invalids_cnpj))
        .transform(lambda df: subtract_by_index(df, df_invalid_ceps))
    )
    
    df = df.dropDuplicates().drop('index')
    
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

#%% Load the function to load the stage data to the stage parquet file
def load_compras_stg(df: DataFrame):
    """Load the stage data to the stage parquet file."""
    # If the project is running with HDFS or another distributed file system, use the commented line below
    #filepath = f'{env("TARGET_PATH")}/compras_stg.parquet'
    #df.write.parquet(filepath, mode='overwrite')
    
    import pandas as pd
    filepath = f'{env("TARGET_PATH")}/compras_stg.parquet'
    pd.DataFrame(df.toPandas()).to_parquet(filepath)
    
#%% Load the function to load the rejected data to the rejected csv file
def load_rejected_compras_stg(df: DataFrame):
    """Load the rejected data to the rejected csv file."""
    
    # If the project is running with HDFS or another distributed file system, use the commented line below
    #filepath = f'{env("TARGET_PATH")}/rejected_compras_stg'
    #df.write.csv(filepath, mode='overwrite', header=True)
    
    import pandas as pd
    filepath = f'{env("TARGET_PATH")}/rejected_compras_stg.csv'
    pd.DataFrame(df.toPandas()).to_csv(filepath, index=False, sep=';')
#%% Job execution
if __name__ == "__main__":
    df, df_rejected = compute_compras_stg(SPARK)
    
    load_compras_stg(df)
    load_rejected_compras_stg(df_rejected)
    
    LOGGER.info("the stage data was loaded successfully. Count: %s", df.count())
    LOGGER.info("the rejected data was loaded successfully. Count: %s", df_rejected.count())
    
