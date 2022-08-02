import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'Aleksandr',    
    'retry_delay': timedelta(minutes=5),
    }

dag_execute_hdfs_commands = DAG(
    dag_id='execute_commands_to_put_in_hdfs',
    default_args=default_args,
    schedule_interval='@once',
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
    description='executing hdfs commands ',
)


# Запускаем тут наш скрипт по скачиванию файла с YC
print("getting df from PostgreSQL")
get_df = BashOperator(
                # YOUR CODE 
        run_as_user='Otus_user'
            )


print("creating a directory")
create_dir = BashOperator(
                task_id="create_dir",
                bash_command="hdfs dfs -mkdir otus_test  ",
            dag=dag_execute_hdfs_commands,
        run_as_user='Otus_user'
            )

print("giving permissions to a directory")
give_permissions = BashOperator(
                task_id="give_permissions",
                bash_command="hdfs dfs -chmod -R 777 otus_test ",  
            dag=dag_execute_hdfs_commands,
        run_as_user='Otus_user'
            )

print("copying from local to hdfs directory")
copy_from_local = BashOperator(
    
            # YOUR CODE 
    
            )

get_df >> create_dir >> give_permissions >> copy_from_local

if __name__ == '__main__ ':
    dag_execute_hdfs_commands.cli()
