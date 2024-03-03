#%% Importing the libraries
from os import getenv as env

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T
from validate_docbr import CNPJ

from external_libs.pyspark_helper import get_pre_configured_glue_session

#%% Initialize the SparkSession
SPARK, SPARK_CONTEXT, GLUE_CONTEXT, JOB, ARGS = get_pre_configured_glue_session({
    "SOURCE_PATH": env("SOURCE_PATH"),
    "WORK_PATH": env("WORK_PATH")
})
#%% Load the function to compute dataframes
def compute_cep(spark: SparkSession, source_path: str) -> DataFrame:
    """Compute the dataframe for the fornecedores table."""
    
    FILEPATH = f'{source_path}/TB_CEP_BR_2018.csv'
    SCHEMA = T.StructType([
        T.StructField('CEP', T.IntegerType(), True),
        T.StructField('UF', T.StringType(), True),
        T.StructField('CIDADE', T.StringType(), True),
        T.StructField('BAIRRO', T.StringType(), True),
        T.StructField('LOGRADOURO', T.StringType(), True),
    ])
    
    return spark.read.csv(FILEPATH, schema=SCHEMA, header=False, sep=';')

def compute_compras_csv(spark: SparkSession, source_path: str) -> DataFrame:
    """Compute the dataframe for the compras table."""
    FILEPATH = f'{source_path}/compras.csv'
    return spark.read.csv(FILEPATH, header=True)
        
def compute_compras_stg(df_compras_csv: DataFrame, df_ceps: DataFrame):
    """Compute the stage data for the table 'compras' and return a DataFrame with the stage data and a rejected DataFrame with the rejected data and the inconsistency type."""

    def subtract_by_index(df: DataFrame, df_to_subtract: DataFrame) -> DataFrame:
        """Subtract the rows of a DataFrame by the index column."""
        return df.join(df_to_subtract, df['index'] == df_to_subtract['index'], 'left_anti').select(df.columns)
    
    df = df_compras_csv

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
        .groupBy(df_compras_csv.columns)
        .agg(
            F.count('*').alias('count')
        )
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

def load_compras_stg(df: DataFrame, target_folder: str):
    """Load the stage data to the stage parquet file.""" 

    filepath = f'{target_folder}/compras.parquet'
    df.write.parquet(filepath, mode='overwrite')

def load_rejected_compras_stg(df: DataFrame, target_folder: str):
    """Load the rejected data to the rejected csv file."""
    
    filepath = f'{target_folder}/rejected_compras_stg.csv'
    df.write.csv(filepath, mode='overwrite', header=True)
#%% Job execution
if __name__ == "__main__":
    df_compras_csv = compute_compras_csv(SPARK, ARGS['SOURCE_PATH'])
    df_ceps = compute_cep(SPARK, ARGS['SOURCE_PATH'])
    df, df_rejected = compute_compras_stg(df_compras_csv, df_ceps)
    
    load_compras_stg(df, ARGS['WORK_PATH'])
    load_rejected_compras_stg(df_rejected, ARGS['WORK_PATH'])