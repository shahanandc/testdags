from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator


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


start = DummyOperator(task_id='run_this_first_1', 
                      executor_config={                            
                          "KubernetesExecutor": {
                              "volumes": [
                                  {
                                   "name": "cpnprdAzureFile",
                                   "azureFile" :
                                   {
                                      "secretName":"cpnprdfilesharepv",
                                      "shareName":"cpmodeldata"
                                   }
                                  }
                              ],
                              "volume_mounts" : [
                                  {
                                      "name":"cpnprdAzureFile",
                                      "mountPath":"/mnt/cpmodeldata"
                                  }
                              ]                              
                          }
                      },
                      dag=dag)

passing = KubernetesPodOperator(namespace='default',
                          image="cpnprdacr.azurecr.io/test/a:v1",
                          image_pull_policy='Always',
                          image_pull_secrets='cpnprdacr',
                          arguments=["test1.R"],
                          labels={"foo": "bar"},
                          name="passing-test1",
                          task_id="passing-task1_1",
                          get_logs=True,
                          dag=dag
                          )

failing = KubernetesPodOperator(namespace='default',
                                executor_config={                            
                                  "KubernetesExecutor": {
                                      "volumes": [
                                          {
                                           "name": "cpnprdAzureFile",
                                           "azureFile" :
                                           {
                                              "secretName":"cpnprdfilesharepv",
                                              "shareName":"cpmodeldata"                                   
                                           }
                                          }
                                      ],
                                      "volume_mounts" : [
                                          {
                                              "name":"cpnprdAzureFile",
                                              "mountPath":"/mnt/cpmodeldata"
                                          }
                                      ]                              
                                  }
                          },      
                          image="ubuntu:1604",
                          cmds=["Python","-c"],
                          arguments=["print('hello world')"],
                          labels={"foo": "bar"},
                          name="fail",
                          task_id="failing-task_1",
                          get_logs=True,
                          dag=dag
                          )

passing.set_upstream(start)
