import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',    
    #'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    # 'email': ['airflow@example.com'],
    # 'email_on_failure': False,
    #'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    #'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag_spark = DAG(
        dag_id = "sparkoperator_demo",
        default_args=default_args,
        # schedule_interval='0 0 * * *',
        schedule_interval='@once',	
        dagrun_timeout=timedelta(minutes=10),
        description='use case of sparkoperator in airflow',
        start_date = airflow.utils.dates.days_ago(1)
)


print("upload df from s3 to local")
upload_df_from_s3 = BashOperator(
#     YOUR CODE
                )

# Выгруженный файл с S3 грузим в hdfs
print("copying from local to hdfs directory")
copy_from_local = BashOperator(
            task_id="copy_from_local",
            bash_command="hdfs dfs -copyFromLocal /home/ubuntu/Otus_Airflow_Pipe/data/BankChurners.csv otus_test",
          dag=dag_spark,
          run_as_user='Otus_user'
            )

# Используем нашу функцию (pyspark_handle_and_upload_csv) по изменению файла с hdfs Pyspark'ом 
print('spark submit task to hdfs')
spark_submit_local = SparkSubmitOperator(
#     YOUR CODE
    )

upload_df_from_s3 >> copy_from_local >> spark_submit_local

if __name__ == '__main__ ':
    dag_spark.cli()



