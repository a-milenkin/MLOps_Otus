import psycopg2
import pandas as pd

def from_yc_to_local():
    
    print('Подключаемся к Postgres на YC')
    conn = psycopg2.connect(
        host='rc1b-8zscp6kk2cb1odhx.mdb.yandexcloud.net',
        port=6432,
        dbname='db1',
        user='user1',
        password='mlopsotus',
        target_session_attrs='read-write',
        sslmode='verify-full')
    
    print('Выгружаем датасет')
    cur = conn.cursor()
    cur.execute("SELECT * FROM otus_train;")
    table = cur.fetchall()
    
    print('Сохраняем датасе в .csv файл через Pandas')
    column_names = [desc[0] for desc in cur.description]
    pd.DataFrame(table, columns = column_names).to_csv('../data/otus_train.csv')
    
    print('Закрываем подключение')
    cur.close()
    conn.close()
    
if __name__ == "__main__":
    from_yc_to_local()
