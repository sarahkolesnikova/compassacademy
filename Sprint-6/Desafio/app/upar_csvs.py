import boto3 #importando a biblioteca boto3
import datetime #importando a biblioteca datetime
import os #importando a biblioteca os

aws_access_key_id= 'access_key' #chave de acesso
aws_secret_access_key = 'secret_access' #chave secreta
#token de sessão
session_token = 'token'
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
