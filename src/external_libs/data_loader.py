from datetime import datetime

from pyspark.sql import DataFrame
from awsglue.context import GlueContext

def merge_dataframe_with_iceberg_table(glue_context: GlueContext, df: DataFrame, database: str, table: str, unique_keys: list[str]):
    """ Merge a DataFrame with an Iceberg table."""
    
    now = datetime.now().strftime('%Y%m%d%H%M%S')
    temp_table_name = f'tmp_{table}_{now}'
    df.createOrReplaceTempView(temp_table_name)
    
    on_clause = ' AND '.join([f'target.{key} = source.{key}' for key in unique_keys])
    sql = f"""
        MERGE INTO glue_catalog.{database}.{table} AS target       
        USING {temp_table_name} AS source
        ON {on_clause}
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """
   
    glue_context.sql(sql)
