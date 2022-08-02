import boto3
import pandas as pd


def upload_from_s3_to_csv():

    session = boto3.session.Session()

    s3 = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net'
    )

    get_object_response = s3.get_object(Bucket='mlopsotus', Key='BankChurners.csv')
    df = pd.read_csv(get_object_response.get("Body"))
    df.to_csv('../data/BankChurners.csv')
    print(df.info())
    
    
if __name__ == '__main__':
    upload_from_s3_to_csv()