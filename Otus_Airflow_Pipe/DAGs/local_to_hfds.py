import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'Aleksandr',    
#     'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    # 'email': ['test@example.com'],
    #'email_on_failure': False,
    #'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    #'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }

dag_execute_hdfs_commands = DAG(
    dag_id='execute_hdfs_commands',
    default_args=default_args,
    # schedule_interval='0 0 * * *',
    schedule_interval='@once',
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
    description='executing hdfs commands ',
)


# start_task = BashOperator(
#                 task_id="start_task",
    
#                 bash_command="/home/ubuntu/Otus_Airflow_Pipe/DAGs/test.sh ",
#           dag=dag_execute_hdfs_commands
#             )

print("creating a directory")
create_dir = BashOperator(
                task_id="create_dir",
                bash_command="hdfs dfs -mkdir /otus_test",
            dag=dag_execute_hdfs_commands,
        run_as_user='Otus_user'
            )

print("giving permissions to a directory")
give_permissions = BashOperator(
                task_id="give_permissions",
                bash_command="hdfs dfs -chmod -R 777 /otus_test ",  
            dag=dag_execute_hdfs_commands,
        run_as_user='Otus_user'
            )

print("listing files in directory")
list_all_files  = BashOperator(
        task_id="list_files",
        bash_command="hdfs dfs -ls /  ",
        dag=dag_execute_hdfs_commands,
        run_as_user='Otus_user'
            )

print("creating files in new directory")
create_empty_file = BashOperator(
                task_id="create_file",
                bash_command="hdfs dfs -touchz /otus_test/test.txt  ",
                dag=dag_execute_hdfs_commands,
                run_as_user='Otus_user'
            )

print("removing directory")
remove_dir = BashOperator(
                task_id="remove_dir",
                bash_command="hdfs dfs -rm -r /test_otus  ",
                dag=dag_execute_hdfs_commands,
                run_as_user='Otus_user'
            )

print("copying from local to directory")
copy_from_local = BashOperator(
            task_id="copy_from_local",
            bash_command="hdfs dfs -copyFromLocal /home/bigdata/Downloads/data_books.txt /otus_test",
          dag=dag_execute_hdfs_commands,
          run_as_user='Otus_user'
            )

# start_task>>
create_dir>>give_permissions>>list_all_files>>create_empty_file>>remove_dir>>copy_from_local

if __name__ == '__main__ ':
    dag_execute_hdfs_commands.cli()
