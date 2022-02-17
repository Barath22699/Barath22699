from datetime import datetime, timedelta
import boto3, json, requests, logging, time
from airflow.exceptions import AirflowException

region_name = 'us-east-1'

def client(region_name):
    global emr
    emr = boto3.client('emr', region_name=region_name)

def get_security_group_id(group_name, region_name):
    ec2 = boto3.client('ec2', region_name=region_name)
    response = ec2.describe_security_groups(GroupNames=[group_name])
    return response['SecurityGroups'][0]['GroupId']
	
def create_cluster(region_name, cluster_name='barath_Cluster_' + str(datetime.now()), release_label='emr-6.2.1',master_instance_type='m5.xlarge', num_core_nodes=2, core_node_instance_type='m5.xlarge'):
    emr_master_security_group_id = get_security_group_id('airflowsecuritygroup', region_name=region_name)
    emr_slave_security_group_id = emr_master_security_group_id
    cluster_response = emr.run_job_flow(
        Name=cluster_name,
        ReleaseLabel=release_label,
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': master_instance_type,
                    'InstanceCount': 1
                },
                {
                    'Name': "Slave nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': core_node_instance_type,
                    'InstanceCount': num_core_nodes
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': True,
            'Ec2KeyName' : 'KeyPairBK',
            'EmrManagedMasterSecurityGroup': emr_master_security_group_id,
            'EmrManagedSlaveSecurityGroup': emr_slave_security_group_id
        },
        BootstrapActions= [
                {
                    'Name': 'Install boto3',
                    'ScriptBootstrapAction': {
                            'Path': 's3://barathlandingzone/Dependencies/dependies.sh',
                        }
                }
            ],
		
	AutoTerminationPolicy = {"IdleTimeout": 120},
        VisibleToAllUsers=True,
        JobFlowRole='barath_emrec2Admin',
        ServiceRole='barath_emrAdmin',
        Applications=[
            { 'Name': 'hadoop' },
            { 'Name': 'spark' },
            { 'Name': 'hive' },
            { 'Name': 'livy' }
        ]
    )
    return cluster_response['JobFlowId']
	
def get_cluster_dns(cluster_id):
    response = emr.describe_cluster(ClusterId=cluster_id)
    return response['Cluster']['MasterPublicDnsName']
	
def wait_for_cluster_creation(cluster_id):
    emr.get_waiter('cluster_running').wait(ClusterId=cluster_id)
	
#Livy
def livy_task(master_dns, spark_config_path, final_code_path, datasetName,dataset_path):
    # 8998 is the port on which the Livy server runs
    host = 'http://' + master_dns + ':8998'
    data = {"file": final_code_path, "className": "com.example.SparkApp","args":[spark_config_path,datasetName,dataset_path]}
    headers = {'Content-Type': 'application/json'}
    #logging.info(f"querying {host}/batches")
    response = requests.post(host + '/batches', data=json.dumps(data), headers=headers)
    logging.info(response.json())
    print(response.headers)
    return response.headers

	
def track_statement_progress(master_dns, response_headers):
    statement_status = ''
    host = 'http://' + master_dns + ':8998'
    session_url = host + response_headers['location'].split('/statements', 1)[0]
    # Poll the status of the submitted scala code
    while statement_status != 'success':
        # If a statement takes longer than a few milliseconds to execute, Livy returns early and provides a statement URL that can be polled until it is complete:
        statement_url = host + response_headers['location']
        statement_response = requests.get(statement_url, headers={'Content-Type': 'application/json'})
        statement_status = statement_response.json()['state']
        logging.info('Statement status: ' + statement_status)

        #logging the logs
        lines = requests.get(session_url + '/log', headers={'Content-Type': 'application/json'}).json()['log']
        for line in lines:
            logging.info(line)

        if 'progress' in statement_response.json():
            logging.info('Progress: ' + str(statement_response.json()['progress']))
        
        if statement_status == 'dead':
            raise AirflowException('Final Statement Status: ' + statement_status)
            
        time.sleep(5)
        
    final_statement_status = statement_response.json()['state']
    return final_statement_status
	
