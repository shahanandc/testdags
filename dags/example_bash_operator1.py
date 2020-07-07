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
  {
  'request_memory': '20Gi',
  'limit_memory': '20Gi'
  }

compute_resources1 = \
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
                          resources={'request_memory': '22Gi', 'limit_memory': '24Gi'},      
                          image_pull_secrets='cpnprdacr',
                          arguments=["/mnt/cpmodeldata/ModelData/a/IOINSTALLVOLUME_Forecasting_Model.R","DEV","YES","NO","NO","NO","7","INS_IP_RES_NONWORKSHARE&INS_IP_RES_WORKSHARE"],
                          labels={"foo": "bar"},
                          name="io_1",
                          task_id="io_1",
                          get_logs=True,
                          volumes=[volume],
                          volume_mounts=[volume_mount],                   
                          dag=dag
                          )

io_2 = KubernetesPodOperator(namespace='default',
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
                          resources={'request_memory': '22Gi', 'limit_memory': '24Gi'},      
                          image_pull_secrets='cpnprdacr',
                          arguments=["/mnt/cpmodeldata/ModelData/a/IOINSTALLVOLUME_Forecasting_Model.R","DEV","YES","NO","NO","NO","7","INS_DTV_FWLL&INS_IP_BUS_ALL"],
                          labels={"foo": "bar"},
                          name="io_2",
                          task_id="io_2",
                          get_logs=True,
                          volumes=[volume],
                          volume_mounts=[volume_mount],                   
                          dag=dag
                          )

io_3 = KubernetesPodOperator(namespace='default',
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
                          resources={'request_memory': '22Gi', 'limit_memory': '24Gi'},      
                          image_pull_secrets='cpnprdacr',
                          arguments=["/mnt/cpmodeldata/ModelData/a/IOINSTALLVOLUME_Forecasting_Model.R","DEV","YES","NO","NO","NO","7","INS_LEGACY"],
                          labels={"foo": "bar"},
                          name="io_3",
                          task_id="io_3",
                          get_logs=True,
                          volumes=[volume],
                          volume_mounts=[volume_mount],                   
                          dag=dag
                          )


io_4 = KubernetesPodOperator(namespace='default',
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
                          resources={'request_memory': '22Gi', 'limit_memory': '24Gi'},      
                          image_pull_secrets='cpnprdacr',
                          arguments=["/mnt/cpmodeldata/ModelData/a/IOREPAIRVOLUME_Forecasting_Model.R","DEV","YES","NO","NO","NO","7","RPR_CABLE&RPRIP_DDCIM"],
                          labels={"foo": "bar"},
                          name="io_4",
                          task_id="io_4",
                          get_logs=True,
                          volumes=[volume],
                          volume_mounts=[volume_mount],                   
                          dag=dag
                          )

io_5 = KubernetesPodOperator(namespace='default',
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
                          resources={'request_memory': '22Gi', 'limit_memory': '24Gi'},      
                          image_pull_secrets='cpnprdacr',
                          arguments=["/mnt/cpmodeldata/ModelData/a/IOREPAIRVOLUME_Forecasting_Model.R","DEV","YES","NO","NO","NO","7","RPRAS_LEGACY&RPROOS_LEGACY"],
                          labels={"foo": "bar"},
                          name="io_5",
                          task_id="io_5",
                          get_logs=True,
                          volumes=[volume],
                          volume_mounts=[volume_mount],                   
                          dag=dag
                          )

io_6 = KubernetesPodOperator(namespace='default',
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
                          resources={'request_memory': '22Gi', 'limit_memory': '24Gi'},      
                          image_pull_secrets='cpnprdacr',
                          arguments=["/mnt/cpmodeldata/ModelData/a/IOREPAIRVOLUME_Forecasting_Model.R","DEV","YES","NO","NO","NO","7","RPR_UVHLP&RPR_IP_WORKSHARE"],
                          labels={"foo": "bar"},
                          name="io_6",
                          task_id="io_6",
                          get_logs=True,
                          volumes=[volume],
                          volume_mounts=[volume_mount],                   
                          dag=dag
                          )
io_7 = KubernetesPodOperator(namespace='default',
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
                          resources={'request_memory': '22Gi', 'limit_memory': '24Gi'},      
                          image_pull_secrets='cpnprdacr',
                          arguments=["/mnt/cpmodeldata/ModelData/a/IOREPAIRVOLUME_Forecasting_Model.R","DEV","YES","NO","NO","NO","7","RPR_IP_NONWORKSHARE&RPR_DTV_FWLL"],
                          labels={"foo": "bar"},
                          name="io_7",
                          task_id="io_7",
                          get_logs=True,
                          volumes=[volume],
                          volume_mounts=[volume_mount],                   
                          dag=dag
                          )
io_1.set_upstream(start)
io_2.set_upstream(start)
io_3.set_upstream(start)

io_4.set_upstream(start)
io_5.set_upstream(start)
io_6.set_upstream(start)
io_7.set_upstream(start)
