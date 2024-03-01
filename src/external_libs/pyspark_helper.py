from sys import argv
from os import getenv as env

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

def __get_spark_conf_with_iceberg_configs(warehouse: str) -> SparkConf:
    conf = SparkConf()
    conf.set("spark.sql.catalog.glue_catalog.warehouse", warehouse)
    conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    conf.set("spark.sql.iceberg.handle-timestamp-without-timezone","true")
    
    return conf

def get_pre_configured_glue_session(args: dict = dict()) -> tuple[SparkSession, SparkContext, GlueContext, Job, dict]:
    """ Return a pre-configured SparkSession, SparkContext, GlueContext, Job and a dictionary with the arguments of the Glue. 
        The GlueCatalog name is: glue_catalog
    
        Args:
            args (dict): A dictionary with the required arguments. The key is the argument name and the value is the default value if the argument is not passed.
            
        Returns:
            tuple[SparkSession, SparkContext, GlueContext, Job, dict]: A tuple with the SparkSession, SparkContext, GlueContext, Job and a dictionary with the arguments of the Glue.
    """

    for key, value in args.items():
        if f'--{key}' not in argv:
            argv.append(f'--{key}')
            argv.append(value)
        
    args = getResolvedOptions(argv, args.keys())    
    
    spark_conf = __get_spark_conf_with_iceberg_configs(
        warehouse = args.get('AWS_WAREHOUSE', env('AWS_WAREHOUSE'))
    )

    spark_context = SparkContext(conf=spark_conf) 
    glue_context = GlueContext(spark_context)
    spark_session = glue_context.spark_session
    job = Job(glue_context)
    
    return spark_session, spark_context, glue_context, job, args