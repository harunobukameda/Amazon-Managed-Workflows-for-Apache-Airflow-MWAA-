import boto3
import time
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator

args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(1),
    "provide_context": False,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries":1,
    "retry_delay":120
    }

##############こちらにご自身のS3バケット名を入れてください################
s3_bucket_name = <your bucket name>
##########################################################################
athena_results = f"s3://{s3_bucket_name}/results/"
output_path = "out0"
input_path = "in0"

athena_drop_output_table_query="DROP TABLE default.handson_output_parquet"

athena_create_input_table_query=f"CREATE EXTERNAL TABLE IF NOT EXISTS default.handson_input_csv(  deviceid string,  uuid bigint,  appid bigint,  country string,  year bigint,  month bigint,  day bigint,  hour bigint)ROW FORMAT DELIMITED  FIELDS TERMINATED BY ','STORED AS INPUTFORMAT  'org.apache.hadoop.mapred.TextInputFormat'OUTPUTFORMAT  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'LOCATION  's3://{s3_bucket_name}/{input_path}/' TBLPROPERTIES (  'classification'='csv',  'delimiter'=',',  'skip.header.line.count'='1' )"

athena_ctas_new_table_query=f"CREATE TABLE \"default\".\"handson_output_parquet\"WITH (format = 'PARQUET',external_location='s3://{s3_bucket_name}/{output_path}/',parquet_compression = 'SNAPPY')AS SELECT *FROM \"default\".\"handson_input_csv\"WHERE deviceid = 'iphone' OR deviceid = 'android'"

def s3_bucket_cleaning_job():
    ##エラー発生追記#######
    ##raise Exception('エラーテスト')
    #######################
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3_bucket_name)
    bucket.objects.filter(Prefix=output_path).delete()

with DAG(
    dag_id="etl_athena_job",
    description="etl athena DAG",
    default_args=args,
    schedule_interval="*/60 * * * *",
    catchup=False,
    tags=['handson']
) as dag:
    t1 = AWSAthenaOperator(task_id="athena_drop_output_table",query=athena_drop_output_table_query, database="default", output_location=athena_results)
    t2 = PythonOperator(task_id="s3_bucket_cleaning", python_callable=s3_bucket_cleaning_job)
    t3 = AWSAthenaOperator(task_id="athena_create_input_table",query=athena_create_input_table_query, database="default", output_location=athena_results)
    t4 = AWSAthenaOperator(task_id="athena_ctas_new_table",query=athena_ctas_new_table_query, database="default", output_location=athena_results)

    t1 >> t3 >> t4
    t2 >> t3 >> t4
