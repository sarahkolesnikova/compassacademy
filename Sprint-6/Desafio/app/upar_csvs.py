import boto3 #importando a biblioteca boto3
import datetime #importando a biblioteca datetime
import os #importando a biblioteca os

aws_access_key_id= 'ASIAQZFG4XPWN2TZ2YPK' #chave de acesso
aws_secret_access_key = 'mFoUFdfsZzzHY9kydxqSjImRehSZdpVytOvPx8fe' #chave secreta
#token de sessão
session_token = 'IQoJb3JpZ2luX2VjEF4aCXVzLWVhc3QtMSJHMEUCIQDdgh3KN7q43nmaC6/3ji31NQYSme5C25EHVRGO6gs7RQIgT6IAAVeB6KJ2gycf5XM084IsK49P4EU5Eb6xzLumuA4qpwMIRhAAGgwwNTQwMzcxMDE1NDgiDDTa3Jm+wb5RYNY7+yqEAyAwzl/XwylMmEqW6S1nu/gMITm4rj8nZfhwdVRvn0MRABSuXFQ5XEncekn3EuBbB9vWK+PECbma3zYlxe8VbWy0k7VWe6BuhhjSSaMUmt77nO9RSWs3buDAeG3GqbMNLaG6qCG6NxZOYbLz7SXw1fE9ftA/tI5L4UvRCE+ECXxkraLZqAeWY4WI5jJ59pe9Q4cv53qXzrpPLngCW/+2HCag12dNgXvcmn4jUMiByBjukCBNLQxO3RH/JwBTKyegrQ2PkCZyRAUid6wcEkp1HfCxghGib1o4KQvuzsBaYNjJ5n+S93Bmb7S4rit92sirTxxrUoB0/snfWpQRW/JMrStur8V5Ft6DxHCe3gWdwkbp7uTd9mKvQl1xw9Lb8RtTxJCXxYRnsQ9mCwWkkoKEWA9ab8zbQFgnlIwQfreA7LXKlcTeVrsJwdHHEqv6nic0Vf+VtH1JLTtiS/kQw2dBDvxEfktnnOEKLIfTtT8SFzCtSXC4DVeJs4Oz+xAPmZRBoz0MvNkw6KzvuwY6pgH/+JfnYfxEuik7oSnlMWOMPtENqbMOF8JwP+ZXVVgbY7gcMHHOzc8Y7WK7bgt4mhvPdI5RE3aEg7Md4jyqIxC/eImHRYN9f6Uy6zNK/ax+V1Tq9krxxMIWeq6RzHyjbDhgn8KRpWxotIpnnpoogvOP/rdwdmUGlc0cSmXixlq8APF/fyjBdGDyqZDqIR6mZaIZnaMJxvc9AcIxJfEQJZtSJVgVXzZB'
bucket_name = 'desafio-final-sarah' # nome do bucket
file_path1 = 'C:/Users/Usuario/Desktop/compassacademy/Sprint-6/Desafio/movies.csv' # caminho do arquivo movies.csv
file_path2 = 'C:/Users/Usuario/Desktop/compassacademy/Sprint-6/Desafio/series.csv' # caminho do arquivo series.csv
# criando o cliente s3
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, aws_session_token=session_token)
try: #criando o bucket
    s3.create_bucket(Bucket=bucket_name)
except Exception as error: #caso o bucket já exista
        print(error)

# fazendo o upload dos arquivos
raw_filmes = f"Raw/Local/CSV/Movies/{datetime.datetime.now().strftime('%Y')}/{datetime.datetime.now().strftime('%m')}/{datetime.datetime.now().strftime('%d')}/"
raw_series = f"Raw/Local/CSV/Series/{datetime.datetime.now().strftime('%Y')}/{datetime.datetime.now().strftime('%m')}/{datetime.datetime.now().strftime('%d')}/"

for file in os.listdir(): #para cada arquivo no diretório
    if file == 'movies.csv': #se o arquivo for movies.csv
        try: #tentar fazer o upload
            s3.upload_file(file, bucket_name, raw_filmes+file)
            print('Upload realizado com sucesso')
        except Exception as error: #caso dê erro
            print(error)
    elif file == 'series.csv': #se o arquivo for series.csv
        try: #tentar fazer o upload
            s3.upload_file(file , bucket_name, raw_series+file)
            print('Upload realizado com sucesso')
        except Exception as error: #caso dê erro
            print(error)
    else: 
        print('Concluido') # ao terminar o upload
