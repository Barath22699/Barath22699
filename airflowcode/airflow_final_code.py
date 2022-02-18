from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3, json, s3fs, time, decimal
import pandas as pd
import pyarrow.parquet as pq
import emr_dependency as emr


def config_data(**kwargs):
    app_config_path = kwargs['dag_run'].conf['app_config_path']
    path_list = app_config_path.replace(":","").split("/")
    
    s3 = boto3.resource(path_list[0])

    obj = s3.Object(path_list[2], "/".join(path_list[3:]))
    data = obj.get()['Body'].read().decode('utf-8')
    json_data = json.loads(data)
    return json_data
    
    
def copy_data(**kwargs):
    ti = kwargs['ti']
    jsonData = ti.xcom_pull(task_ids='config_data')
    datasetName = kwargs['dag_run'].conf['datasetName']
    dataset_path = kwargs['dag_run'].conf['dataset_path']
    source_path_list = (jsonData['ingest-dataset']['source']['data-location']+dataset_path).replace(":","").split("/")
    dest_path_list = (jsonData['ingest-dataset']['destination']['data-location']+dataset_path).replace(":","").split("/")
    print(source_path_list, dest_path_list)
    s3_client = boto3.client(source_path_list[0])

    print(f"Copying from {source_path_list[2] + '/'+ '/'.join(source_path_list[3:])} to {dest_path_list[2] + '/'+ '/'.join(dest_path_list[3:])}")
    s3_client.copy_object(
        CopySource = {'Bucket': source_path_list[2], 'Key': "/".join(source_path_list[3:])},
        Bucket = dest_path_list[2],
        Key = "/".join(dest_path_list[3:])
        )

            
def pre_validation(**kwargs):
    ti = kwargs['ti']
    jsonData = ti.xcom_pull(task_ids='config_data')
    dataset_path = kwargs['dag_run'].conf['dataset_path']
    s3 = s3fs.S3FileSystem()
    
    df_landingzone = pq.ParquetDataset(jsonData['ingest-dataset']['source']['data-location']+dataset_path, filesystem=s3).read_pandas().to_pandas()
    df_rawzone = pq.ParquetDataset(jsonData['ingest-dataset']['destination']['data-location']+dataset_path, filesystem=s3).read_pandas().to_pandas()
    
    if df_rawzone[df_rawzone.columns[0]].count() != 0: # df.columns[0] = 1st column
        for raw_columnname in df_rawzone.columns:
            if df_rawzone[raw_columnname].count() == df_landingzone[raw_columnname].count():
                print('count satisfied with',str(raw_columnname))
            else:
                raise ValueError("Count mismatch for", str(raw_columnname))
    else:
        raise ValueError("No Data Available in the file")

# Creates an EMR cluster
def create_cluster(**kwargs):
    region = 'us-east-1'
    emr.client(region_name=region)
    cluster_id = emr.create_cluster(region_name=region, cluster_name='Barath_Cluster', num_core_nodes=1)
    return cluster_id
    
def waiting_for_cluster_to_start(**kwargs):
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr.wait_for_cluster_creation(cluster_id)
    
def livy_submit(**kwargs):
    spark_config_path = kwargs['dag_run'].conf['spark_config_path']
    final_code_path = kwargs['dag_run'].conf['final_code_path']
    datasetName = kwargs['dag_run'].conf['datasetName']
    dataset_path = kwargs['dag_run'].conf['dataset_path']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    
    cluster_dns = emr.get_cluster_dns(cluster_id)
    headers = emr.livy_task(cluster_dns, spark_config_path, final_code_path, datasetName, dataset_path)
    session_status = emr.track_statement_progress(cluster_dns, headers)
    return session_status
    
def post_validation(**kwargs):
    ti = kwargs['ti']
    jsonData = ti.xcom_pull(task_ids='config_data')
    dataset_path = kwargs['dag_run'].conf['dataset_path']
    s3 = s3fs.S3FileSystem()
    
    df_rawzone = pq.ParquetDataset(jsonData['masked-dataset']['source']['data-location']+("/".join(dataset_path.split("/")[:len(dataset_path.split("/"))-1])), filesystem=s3).read_pandas().to_pandas()
    df_stagingzone = pq.ParquetDataset(jsonData['masked-dataset']['destination']['data-location']+("/".join(dataset_path.split("/")[:len(dataset_path.split("/"))-1])), filesystem=s3).read_pandas().to_pandas()
    
    
    # Availability check
    
    if df_stagingzone[df_stagingzone.columns[0]].count() != 0: # df.columns[0] = 1st column
        # Count check
        for raw_columnname in df_stagingzone.columns:
            if df_stagingzone[raw_columnname].count() == df_rawzone[raw_columnname].count():
                print('count satisfied with',str(raw_columnname))
            else:
                raise ValueError("Count mismatch for", str(raw_columnname))
                
        # Datatype validation
        for i in jsonData['masked-dataset']['transformation-cols']:
            if jsonData['masked-dataset']['transformation-cols'][i].split(',')[0] == "DecimalType":
                if isinstance(df_stagingzone[i][0],decimal.Decimal) & (str(abs(decimal.Decimal(df_stagingzone[i][0]).as_tuple().exponent)) == jsonData['masked-dataset']['transformation-cols'][i].split(',')[1]):
                    print('Datatype matches for '+ i)
                else:
                    raise TypeError("Datatype Mismatch in "+i)
            if jsonData['masked-dataset']['transformation-cols'][i].split(',')[0] == "ArrayType-StringType" or jsonData['masked-dataset']['transformation-cols'][i].split(',')[0] == "StringType":
                if pd.api.types.is_string_dtype(df_stagingzone[i]):
                    print('Datatype matches for '+ i)
                else:
                    raise TypeError("Datatype Mismatch in "+i)
    else:
        raise ValueError("No Data Available in the file")
    
dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 2, 1),
    'email': ['barath.srinivasa@tigeranalytics.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
    }

dag = DAG(
    dag_id='final_dag',
    start_date=datetime(2022, 2, 1),
    default_args=dag_args,
    max_active_runs=1,
    end_date=None,
    catchup=False,
    schedule_interval='@once')
    
    
config_data = PythonOperator(
        task_id="config_data",
        python_callable=config_data,
        dag=dag)
        
copy_data = PythonOperator(
        task_id="copy_data",
        python_callable=copy_data,
        dag=dag)
        
pre_validation = PythonOperator(
        task_id="pre_validation",
        python_callable=pre_validation,
        dag=dag)
        
create_cluster = PythonOperator(
        task_id="create_cluster",
        python_callable=create_cluster,
        dag=dag)
        
waiting_for_cluster_to_start = PythonOperator(
        task_id="waiting_for_cluster_to_start",
        python_callable=waiting_for_cluster_to_start,
        dag=dag)  
        
livy_submit = PythonOperator(
        task_id="livy_submit",
        python_callable=livy_submit,
        dag=dag)
        
post_validation = PythonOperator(
        task_id="post_validation",
        python_callable=post_validation,
        dag=dag)
        
        
config_data >> copy_data >> pre_validation >> create_cluster >> waiting_for_cluster_to_start >> livy_submit >> post_validation
