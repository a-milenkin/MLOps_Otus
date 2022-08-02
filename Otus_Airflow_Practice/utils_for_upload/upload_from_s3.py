# Подключаемся к S3 на YandexCloud
# https://cloud.yandex.com/en-ru/docs/storage/tools/boto

# Выгружаем датасет и кладем его в ../data/BankChurners.csv
# Для этого нужны всего лишь две библиотеки

import boto3
import pandas as pd


def upload_from_s3_to_csv():

    # Создаем сессию
    session = boto3.session.Session()

    s3 = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net'
    )
        
    # Подключаемся к сервису s3 (Object Storage). Ниже вспомогательная информация
    
#     service_name='s3'
#     endpoint_url='https://storage.yandexcloud.net'    

    # YOUR CODE
    
    get_object_response = s3.get_object(Bucket='mlopsotus', Key='BankChurners.csv')
    
    
    # Забираем объект с бакета mlopsotus из забираем объект BankChurners
    
    # YOUR CODE
    
    # С помощью метода get("Body") получает содержимое объекта и кладем в Pandas DataFrame
    
    df = pd.read_csv(get_object_response.get("Body"))
    
    # YOUR CODE
        
#     Сохраняем 
    df.to_csv('../data/BankChurners.csv')
    
    
if __name__ == '__main__':
    upload_from_s3_to_csv()