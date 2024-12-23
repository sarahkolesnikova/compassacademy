import boto3 
from botocore.exceptions import NoCredentialsError
aws_acess_key_id = 'ASIAQZFG4XPWIIDL7G2P'
aws_secret_acess_key = 'n6Zf2VBDjeEnbWACl+RWwFe7lBxoyyfQA+9L+JG9'
session_token = 'IQoJb3JpZ2luX2VjELj//////////wEaCXVzLWVhc3QtMSJIMEYCIQDpZiAoMRQckwlWJICh5IRn7kslLjiCHvBBJNzufWHRuQIhANfIn8znT5MXeMOQZERLYa3KgLpRM3Qw/53/D/kQ2rBcKrADCID//////////wEQABoMMDU0MDM3MTAxNTQ4IgyjBC/6IcooD3gsRYkqhAOjv4yB2i6KL7pr67wJhhoi1gEQJlRrDWDLtniamuru2u5mohxiT/2wXzBrnl6SK0XhGbh2P9OyV85f/Lcb1OwuX+OxISsQaZxZk3GJ6Dg8IqmqgIo177uJidS0E5AeSPk2yt5CWzOuzOrKQq6PpgZ2yl3mjRxnJ95tnCOFNnhYIFqdh7k4QZar9tadw/eiK6VvAV/ZYaRGucexKNRZ+dR3AsQtZ76svNFQu4mDCNr2cs3xN1qjZSbjdIkirWksaQBzhip/JZwuaajpl3PDOOIxe0AdCpNcCsDq/8ixGlg4qPYvBCNr09vecZYgoFEfcmDDH9KG2oSBFWNL0mNcFFque66ARC8UlBRvYQe6aYuFRDZN8yBuCzyT1sd8PzwN0HOXkCNEqkvudSL2eQtF4luQhkT7y7zAvZzea/YvXS6olIWOoeVOudfZ54XFcua2wf0lbpv4QIsJ9X6IfHzSzZ2w3RZZcgMsVCWSTv9cSTgLcfoH8Bf4EevEywNISPal9+QMfMIWMI7QkrsGOqUBj98jGKPtUoGXjLBIwz2w8wJOBSOc1jo0ykv5JcA/3qvZCWJYqn/3Hx877Dy+75kJ9OfUbP06Mt5vCK4GcsN/hPE4CZozMB6EBLFafiXM5F6ghaG0n/IAEWKoHntYJWThHPL9wABr83b/xgVZv0lL0mu7CDXAhXROfdflqhqAVh02IT4gg2d966iKCS6WLeun/WNcSyxv0dk9088ECSp56zhlNukG'
bucket_name = 'desafio-sprint-5-sarah'
file_path = 'C:/Users/Usuario/Desktop/compassacademy/Sprint-5/Desafio/op_por_invest_2024.csv'
object_name = 'op_por_invest_2024.csv'

#criar backet

def up_para_aws(aws_access_key_id, aws_secret_access_key, session_token, bucket_name, file_path, object_name):
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, aws_session_token=session_token)
    try:
        s3.upload_file(file_path, bucket_name, object_name)
        print("Upload Successful")
    except FileNotFoundError:
        print("The file was not found")
    except NoCredentialsError:
        print("Credentials not available")
    except Exception as e:
        print(e)


up_para_aws(aws_acess_key_id, aws_secret_acess_key, session_token, bucket_name, file_path, object_name)
   