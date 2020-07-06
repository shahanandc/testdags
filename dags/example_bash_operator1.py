from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount

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
  {
  'request_memory': '20Gi',
  'limit_memory': '20Gi'
  }

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
                      bash_command="mkdir /mnt/cpmodeldata/ModelData/a",
                      xcom_push=True,
                      dag=dag)

iefs_install_train = KubernetesPodOperator(namespace='default',
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
                          resources=compute_resources,      
                          image_pull_secrets='cpnprdacr',
                          arguments=["/mnt/cpmodeldata/ModelData/IEFSINSTALLVOLUME/code/IEFSINSTALLVOLUME_Training_Model.R"],
                          labels={"foo": "bar"},
                          name="iefs_install_train",
                          task_id="iefs_install_train",
                          get_logs=True,
                          volumes=[volume],
                          volume_mounts=[volume_mount],                   
                          dag=dag
                          )

iefs_repair_train = KubernetesPodOperator(namespace='default',
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
                          resources=compute_resources,      
                          image_pull_secrets='cpnprdacr',
                          arguments=["/mnt/cpmodeldata/ModelData/IEFSREPAIRVOLUME/code/IEFSREPAIRVOLUME_Training_Model.R"],
                          labels={"foo": "bar"},
                          name="iefs_repair_train",
                          volumes=[volume],
                          volume_mounts=[volume_mount],                   
                          task_id="iefs_repair_train",
                          get_logs=True,
                          dag=dag
                          )

iefs_install_train.set_upstream(start)
iefs_repair_train.set_upstream(start)

