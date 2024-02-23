from os import getenv as env
from typing import TypedDict

from pyspark.sql import DataFrame
import pymssql 

def _create_sqlalchemy_conection_with_temptable(df: DataFrame, table_name: str ):
    """Creates a connection to a mssql database and a temp table to store the data from a dataframe. The table created has the same columns and equivalents datatypes as the dataframe."""
    db_server_name = env('DB_SERVER_NAME')
    db_port = env('DB_PORT')
    db_name = env('DB_NAME')
    db_user = env('DB_USER')
    db_password = env('DB_PASSWORD')
    
    conn = pymssql.connect(db_server_name, db_user, db_password, db_name)
    
    spark_to_sqlserver_datatypes = {
        "ByteType": "tinyint",
        "ShortType": "smallint",
        "IntegerType": "int",
        "LongType": "bigint",
        "FloatType": "real",
        "DoubleType": "float",
        "DecimalType": "decimal",
        "StringType": "nvarchar(max)",  
        "BinaryType": "varbinary(max)",
        "BooleanType": "bit",
        "TimestampType": "datetime2",
        "DateType": "date",
        "ArrayType": "varbinary(max)",  
        "MapType": "varbinary(max)", 
        "StructType": "varbinary(max)",  
    }
    
    temp_table_columns: list[tuple[str, str]] = []
    
    for column in df.schema:
        column_name = column.name
        
        # If column_type have parenthesis in the end, remove it
        
        column_type = str(column.dataType).split('(')[0]
        
        converted_type = spark_to_sqlserver_datatypes[column_type]
        temp_table_columns.append(tuple([column_name, converted_type]))
    
    column_and_type = ','.join([f'{column[0]} {column[1]}' for column in temp_table_columns])
    temp_table_name = f"##{table_name}"
    
    sql = f"CREATE TABLE {temp_table_name} ({column_and_type})"
    conn.cursor().execute(sql)
    conn.commit()
    
    return conn, temp_table_name
    
def insert_into_mssql(df: DataFrame, table_name: str):
    """Insert a dataframe into a mssql table using a temp table to store the data using INSERT INTO SELECT statement"""
    connection, temp_table_name = _create_sqlalchemy_conection_with_temptable(df, table_name)
    
    df.write.format("jdbc") \
        .option("url", f"""jdbc:sqlserver://{env('DB_SERVER_NAME')}:1433;databaseName={env('DB_NAME')};encrypt=false""") \
        .option("dbtable", f'dbo.{temp_table_name}') \
        .option("user", env("DB_USER")) \
        .option("password", env("DB_PASSWORD")) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .mode("append") \
        .save()
    
    columns = ','.join(df.columns)
    sql = f"INSERT INTO {table_name} ({columns}) SELECT {columns} FROM {temp_table_name}"
    
    connection.cursor().execute(sql)
    connection.commit()

class MergeStatistics(TypedDict):
    """Merge statistics for a merge operation in a mssql table"""
    INSERT: int
    UPDATE: int

def merge_into_mssql(df: DataFrame, table_name: str, unique_keys: list[str]):
    """Execute a upsert (merge statement) into a mssql table using a temp table to store the data"""
    connection, temp_table_name = _create_sqlalchemy_conection_with_temptable(df, table_name)
    
    result = None
    
    try:
        df.write.format("jdbc") \
            .option("url", f"""jdbc:sqlserver://{env('DB_SERVER_NAME')}:1433;databaseName={env('DB_NAME')};encrypt=false""") \
            .option("dbtable", f'dbo.{temp_table_name}') \
            .option("user", env("DB_USER")) \
            .option("password", env("DB_PASSWORD")) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .mode("append") \
            .save()
            
        columns = ','.join(df.columns)
        
        sql = f"""
            CREATE TABLE #MergeLog (MergeAction nvarchar(10));
            
        
            MERGE INTO {table_name} AS target
            USING {temp_table_name} AS source
            ON ({f' AND '.join([f'target.{key} = source.{key}' for key in unique_keys])})
            
            WHEN MATCHED THEN
                UPDATE SET {f', '.join([f'target.{column} = source.{column}' for column in df.columns])}
            
            WHEN NOT MATCHED THEN
                INSERT ({columns}) VALUES ({', '.join([f'source.{column}' for column in df.columns])})
                
            OUTPUT $action into #MergeLog;
        """
        
        cursor = connection.cursor()
        cursor.execute(sql)
        connection.commit()
        
        sql = """
            SELECT MergeAction, Cnt=count(*)
            FROM   #MergeLog
            GROUP BY MergeAction;
        """
        cursor.execute(sql)
        result = cursor.fetchall() 
        connection.close()    
    except Exception as e:
        connection.close()
        raise e
    
    statistics = dict(result)
    
    return MergeStatistics(
        INSERT=statistics.get('INSERT', 0),
        UPDATE=statistics.get('UPDATE', 0)
    )
    