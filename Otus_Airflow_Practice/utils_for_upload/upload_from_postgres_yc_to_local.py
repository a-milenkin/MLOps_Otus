# Подключаемся к PostgreSQL на YandexCloud
# https://console.cloud.yandex.com/folders/b1gkp4u1i2d4kqbnrhpp/managed-postgresql/cluster/c9qmrk6bhi104h70i49v?section=overview

# Выгружаем датасет и кладем его в ../data/otus_train.csv
# Для этого нужны всего лишь две библиотеки
import psycopg2
import pandas as pd

# Пусть сама фукнция будет иметь следующую структуру

def upload_from_postgres_yc_to_local():
    
    print('Подключаемся к Postgres на YC')
    
    # Вспомогательная информация, чтоб скачать мой датасет
#     host='rc1b-8zscp6kk2cb1odhx.mdb.yandexcloud.net'
#     port=6432
#     dbname='db1'
#     user='user1'
#     password='mlopsotus'
#     target_session_attrs='read-write'
#     sslmode='verify-full'
    
    # YOUR CODE 
    
    print('Создаем сессию и курсор')
    
    # YOUR CODE 
    
    print('Выгружаем датасет в csv через pandas и заполняем название столбцов')
    
    # YOUR CODE
    
    print('Сохраняем датасе в .csv файл через Pandas')
    pd.DataFrame(table, columns = column_names).to_csv('../data/otus_train.csv')
    
    # YOUR CODE
    
    print('Закрываем курсор и подключение')
    cur.close()
    conn.close()
    
if __name__ == "__main__":
    upload_from_postgres_yc_to_local()
