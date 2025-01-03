import boto3 
from botocore.exceptions import NoCredentialsError
aws_acess_key_id = 'ASIAQZFG4XPWAICQFIZ5' #chave de acesso
aws_secret_acess_key = 'mlQhwEA/jkXrOyi0bGQnhtwGgoja9ENfYrOopeSd' #chave secreta
#token de sessão
session_token = 'IQoJb3JpZ2luX2VjEBEaCXVzLWVhc3QtMSJIMEYCIQCYmVYHRouvoTKJfFWCxd3PLULlkAF0/48SKSuTFkqqOAIhANLwgeRPXCmY5oH89D9AWFhNlbLB6ibjyuq8u13iGWxWKrADCNr//////////wEQABoMMDU0MDM3MTAxNTQ4IgzIFcbrvxoZgt1rRPgqhANcJS51DkLaM5v7GOPU7OgvXGdx72G5ChexOeNWEWJZfWeTcqe+y2Bf3zv2gvYgg/MLDrY73x7KUsZxfsPnTZWnKv0Xdsqfg1Xni+dDxTdfIApnhJxcjYZ8JYXtehwmDDtApZNVae40XW40hpqDy/Nq5aP4uN5VZGsAdtASgd/NdEuNFQ8pONNx4RHwDQBppdlkGlf+lgDN52kDbqHH5YyhJOZP+z1TotZ+0nit+zi72S3yj8jjcyiVVhTEPdt/5++KEjtydUJUr63LlQGfQ4u3a7vnxseBQvB96VmdJdLdav8bq7mpwNExK7Dewwrjl1klqti/tHWKUOqtAURstXBQkizkLj+pERh18plaU1HYzgrnFwX9OynZDMzZeb1/Y2L/7qnREEP+wQVrqBGjJVmT75vbguCN7ZHMrb3rj580rUuKocSmDCaYuykBWE9qh8AcHTQRV+Ghoe7nFlwP8eENqjCniI2d78AGu1Cg+RIE0fgU87FG0R2lVawCkUQhWlHoGbwAMLmhprsGOqUBETJZLJeudK4yANYn1ywYgdG2MQLMcVwOD+UGxHjjAMFL/KyRSZmwgn1YErqC4/vVdTMnWjly8+TCtdElZeA/osml1nQnW4bZPNxfRVkVfRqVTIr2pb7miN0FrZNIUdzEjh4ORfaGMGEWxP5NHJ5FkjqIUKP9SmqztQJU/mlpPMX3Pnp897cvQBP5KR7gMWTfkdWYJYEHhyvgNZujQoLl33W4EvbV'
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
   