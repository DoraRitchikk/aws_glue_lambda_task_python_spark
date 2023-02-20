import sys
from awsglue.transforms import *
from contextlib import contextmanager
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F
from pyspark.sql import Window
import pyspark.sql.types as G
from awsglue.dynamicframe import DynamicFrame

INPUT_RAW_FLIGHTS_DATA_S3_PATH = 'input_bucket'
OUTPUT_CONNECTION = 'output_connection'
GLUE_OUTPUT_DB = 'postgres'
GLUE_OUTPUT_SCHEMA = 'public'

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', INPUT_RAW_FLIGHTS_DATA_S3_PATH,
    OUTPUT_CONNECTION
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

GLUE_INPUT_S3_BUCKET = args[INPUT_RAW_FLIGHTS_DATA_S3_PATH]

GLUE_OUTPUT_CONNECTION_NAME = args[OUTPUT_CONNECTION]

# Extract
df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [GLUE_INPUT_S3_BUCKET],
        "recurse": True,
    }
).toDF()

#Transform
#CRSDepTime
df2 = df.withColumn("CRSDepTime",F.col("CRSDepTime").cast(F.StringType()))
df3 = df2.withColumn("DepTime",F.col("DepTime").cast(F.StringType()))

filter_with_float = df3.withColumn("DepTime",F.regexp_replace(F.col("DepTime"),".0",""))

filter_df = filter_with_float.where(F.length(F.col("CRSDepTime")) == 3).withColumn("CRSDepTime",F.concat(F.lit("0"),F.col("CRSDepTime")))
filter_three = filter_df.where(F.length(F.col("DepTime")) == 3).withColumn("DepTime",F.concat(F.lit("0"),F.col("DepTime")))

#DepTime
filter_df_dep = filter_with_float.where(F.length(F.col("DepTime")) == 3).withColumn("DepTime",F.concat(F.lit("0"),F.col("DepTime")))
filter_three_crs = filter_df_dep.where(F.length(F.col("CRSDepTime")) == 3).withColumn("CRSDepTime",F.concat(F.lit("0"),F.col("CRSDepTime")))

#Union CRS and Dep
res_df = filter_three.union(filter_three_crs).where((F.length(F.col("CRSDepTime")) > 3) & (F.length(F.col("DepTime")) > 3))

#Time format
result_df_f = res_df.withColumn("CRSDepTime",F.unix_timestamp('CRSDepTime','HHmm')).withColumn('CRSDepTime',F.from_unixtime('CRSDepTime','HH:mm'))
result_df = result_df_f.withColumn("DepTime",F.unix_timestamp('DepTime','HHmm')).withColumn('DepTime',F.from_unixtime('DepTime','HH:mm'))

#Add a coolumn
addColDF = result_df.withColumn("DelayGroup",F.lit(None))

new_res_early = addColDF.where(F.col("DepDelayMinutes") == 0).withColumn("DelayGroup",F.lit("OnTime_Early"))
new_res_small = addColDF.where((F.col("DepDelayMinutes") > 0) & (F.col("DepDelayMinutes") <= 15)).withColumn("DelayGroup",F.lit("Small_Delay")).union(new_res_early)
new_res_medium = addColDF.where((F.col("DepDelayMinutes") > 0) & (F.col("DepDelayMinutes") <= 45)).withColumn("DelayGroup",F.lit("Medium_Delay")).union(new_res_small)
new_res_large = addColDF.where(F.col("DepDelayMinutes") > 45).withColumn("DelayGroup",F.lit("Large_Delay")).union(new_res_medium)
new_res_final = addColDF.where(F.col("Cancelled")).withColumn("DelayGroup",F.lit("Cancelled")).union(new_res_large)

#LOAD

@contextmanager
def get_output_connection():
    from py4j.java_gateway import java_import
    java_import(sc._gateway.jvm, "java.sql.Connection")
    java_import(sc._gateway.jvm, "java.sql.DatabaseMetaData")
    java_import(sc._gateway.jvm, "java.sql.DriverManager")
    java_import(sc._gateway.jvm, "java.sql.SQLException")
    target_jdbc_conf = glueContext.extract_jdbc_conf(GLUE_OUTPUT_CONNECTION_NAME)
    connection = sc._gateway.jvm.DriverManager.getConnection(
        f"{target_jdbc_conf.get('url')}/{GLUE_OUTPUT_DB}",
        target_jdbc_conf.get('user'),
        target_jdbc_conf.get('password')
    )
    try:
        yield connection
    finally:
        connection.close()


def purge_table(table):
    with get_output_connection() as connection:
        stmt = connection.createStatement()
        stmt.executeUpdate(f'DROP TABLE IF EXISTS {table}')

def write_to_db(df, table):
    purge_table(table)
    target_jdbc_conf = glueContext.extract_jdbc_conf(GLUE_OUTPUT_CONNECTION_NAME)
    options = {
        'user': target_jdbc_conf.get('user'),
        'password': target_jdbc_conf.get('password'),
        'connectionType': 'postgresql',
        'url': f"{target_jdbc_conf.get('url')}/{GLUE_OUTPUT_DB}",
        'dbtable': table
    }
    dynamic_frame = DynamicFrame.fromDF(df, glueContext)
    glueContext.write_dynamic_frame.from_options(
        dynamic_frame,
        connection_type="postgresql",
        connection_options=options
    )


write_to_db(new_res_final, f'{GLUE_OUTPUT_SCHEMA}.flights')

# Partition section
statistics_df = df.select('Airline', 'AirTime', 'DepDelay').groupBy('Airline').agg(
    F.mean('AirTime').alias('mean_Airtime'), F.mean('DepDelay').alias('mean_Delay'))

group_byGroup = df.groupBy('Airline', 'DelayGroup').count()
window = Window.partitionBy("Airline").orderBy(F.desc("count"))
most_common_delay_groups_df = group_byGroup.withColumn('gr', F.row_number().over(window)).where(F.col('gr') == 1)
statistics_with_delay_groups_df = statistics_df.join(most_common_delay_groups_df.select('Airline', 'DelayGroup'),
                                                     "Airline").dropDuplicates()

# Add column with most frequent rote
groupByRoutes = df.groupBy('Airline', 'Origin', 'Dest').count()
winRoutes = Window.partitionBy('Airline').orderBy(F.desc("count"))
most_common_flightways_df = groupByRoutes.withColumn('group', F.row_number().over(winRoutes)).where(F.col("group") == 1)

res = statistics_with_delay_groups_df.join(resRoute.select('Airline', 'Origin', 'Dest'), "Airline").dropDuplicates()

res.write.mode("overwrite").format("json").partitionBy('Airline').save('s3://dritchik-education/statistics/')

job.commit()