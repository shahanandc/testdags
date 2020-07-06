from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from airflow.contrib.kubernetes.pod import Resources

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': '2020-07-03',
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'example_bash_operator1', default_args=default_args, schedule_interval=timedelta(minutes=10000))

compute_resources = \
  {'request_cpu': '1000m',
  'request_memory': '21Gi',
  'limit_cpu': '1000m',
  'limit_memory': '21Gi'}

pod_resources = Resources()
pod_resources.request_cpu = '1000m'
pod_resources.request_memory = '21Gi'
pod_resources.limit_cpu = '2000m'
pod_resources.limit_memory = '21Gi'

volume = Volume(
    name="cpnprdazurefile",
    configs={
        "azureFile" :
         {
           "secretName":"cpnprdfilesharepv",
           "shareName":"cpmodeldata"
         }
    }
)

volume_mount = VolumeMount(
    "cpnprdazurefile",
    mount_path="/mnt/cpmodeldata",   
    sub_path=None,
    read_only=False
)

start = BashOperator(task_id='run_this_first_1', 
                      executor_config={                            
                          "KubernetesExecutor": {
                              "volumes": [
                                  {
                                   "name": "cpnprdazurefile",
                                   "azureFile" :
                                   {
                                      "secretName":"cpnprdfilesharepv",
                                      "shareName":"cpmodeldata"
                                   }
                                  }
                              ],
                              "volume_mounts" : [
                                  {
                                      "name":"cpnprdazurefile",
                                      "mountPath":"/mnt/cpmodeldata"
                                  }
                              ]                              
                          }
                      },
                      bash_command="mkdir -p /mnt/cpmodeldata/ModelData/a",
                      xcom_push=True,
                      dag=dag)

io_1 = KubernetesPodOperator(namespace='default',
                            executor_config={                            
                              "KubernetesExecutor": {
                                  "volumes": [
                                      {
                                       "name": "cpnprdazurefile",
                                       "azureFile" :
                                       {
                                          "secretName":"cpnprdfilesharepv",
                                          "shareName":"cpmodeldata"
                                       }
                                      }
                                  ],
                                  "volume_mounts" : [
                                      {
                                          "name":"cpnprdazurefile",
                                          "mountPath":"/mnt/cpmodeldata"
                                      }
                                  ]                              
                              }
                          },      
                          image="cpnprdacr.azurecr.io/test/a:v1",
                          image_pull_policy='Always',
                          #resources={'request_memory': '22Gi', 'limit_memory': '24Gi'},      
                          image_pull_secrets='cpnprdacr',
                          arguments=["/mnt/cpmodeldata/ModelData/a/IOINSTALLVOLUME_Forecasting_Model.R"],
                          labels={"foo": "bar"},
                          name="io_1",
                          task_id="io_1",
                          get_logs=True,
                          volumes=[volume],
                          volume_mounts=[volume_mount],                   
                          dag=dag
                          )



io_1.set_upstream(start)


