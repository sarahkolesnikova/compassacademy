#Qual o perfil dos investidores que aderiram ao tesouro direto em 2024 e operaram comprando no primeiro semestre?

import pandas as pd
import polars as pl
import boto3 
op_por_invest = pd.read_csv('op_por_invest_2024.csv' , sep=',')
op_por_invest

op_por_invest.info()

op_por_invest.isna().sum()

# Função de conversão
op_por_invest['Data de Adesao'] = op_por_invest['Data de Adesao'].astype(str)

#uma função de string
op_por_invest.rename(columns={'Data de Adesao': 'Ultima Data de Adesao'}, inplace=True)


# Função de conversão
op_por_invest['Data da Operacao'] = pd.to_datetime(op_por_invest['Data da Operacao'])

# Função de conversão
op_por_invest['Ultima Data de Adesao'] = pd.to_datetime(op_por_invest['Ultima Data de Adesao'], format='%d/%m/%Y')

op_por_invest['Valor da Operacao'] = op_por_invest['Valor da Operacao'].str.replace(',', '.').astype(float)

op_por_invest.info()

op_por_invest

# Uma clausula que filtra com dois operadores logicos
compras_1_semestre = op_por_invest[(op_por_invest['Data da Operacao'] >= '2024-01-01') & (op_por_invest['Data da Operacao'] <= '2024-06-30') & (op_por_invest['Tipo da Operacao'] == 'C')]
compras_1_semestre 

# Uma função de data
compras_1_semestre['Mes da Operacao'] = compras_1_semestre['Data da Operacao'].dt.month
compras_1_semestre

# Duas funções de agregação
agg_df = compras_1_semestre .groupby(['Tipo Titulo', 'Mes da Operacao', 'Genero', 'Profissao', 'UF do Investidor' ]).agg({'Idade': 'mean', 'Quantidade': 'count'}).reset_index()
agg_df

agg_df['Quantidade'].mean()

# Uma função condicional
agg_df['Classificação'] = agg_df['Quantidade'].apply(lambda x: 'Alto' if x > 40 else 'Intermediario' if 21 < x <= 40 else 'Baixo')
agg_df


agg_df.to_csv('perfil_investidores.csv', sep = ';', index=False)


from botocore.exceptions import NoCredentialsError
aws_acess_key_id = 'ASIAQZFG4XPWAICQFIZ5'
aws_secret_acess_key = 'mlQhwEA/jkXrOyi0bGQnhtwGgoja9ENfYrOopeSd'
session_token = 'IQoJb3JpZ2luX2VjEBEaCXVzLWVhc3QtMSJIMEYCIQCYmVYHRouvoTKJfFWCxd3PLULlkAF0/48SKSuTFkqqOAIhANLwgeRPXCmY5oH89D9AWFhNlbLB6ibjyuq8u13iGWxWKrADCNr//////////wEQABoMMDU0MDM3MTAxNTQ4IgzIFcbrvxoZgt1rRPgqhANcJS51DkLaM5v7GOPU7OgvXGdx72G5ChexOeNWEWJZfWeTcqe+y2Bf3zv2gvYgg/MLDrY73x7KUsZxfsPnTZWnKv0Xdsqfg1Xni+dDxTdfIApnhJxcjYZ8JYXtehwmDDtApZNVae40XW40hpqDy/Nq5aP4uN5VZGsAdtASgd/NdEuNFQ8pONNx4RHwDQBppdlkGlf+lgDN52kDbqHH5YyhJOZP+z1TotZ+0nit+zi72S3yj8jjcyiVVhTEPdt/5++KEjtydUJUr63LlQGfQ4u3a7vnxseBQvB96VmdJdLdav8bq7mpwNExK7Dewwrjl1klqti/tHWKUOqtAURstXBQkizkLj+pERh18plaU1HYzgrnFwX9OynZDMzZeb1/Y2L/7qnREEP+wQVrqBGjJVmT75vbguCN7ZHMrb3rj580rUuKocSmDCaYuykBWE9qh8AcHTQRV+Ghoe7nFlwP8eENqjCniI2d78AGu1Cg+RIE0fgU87FG0R2lVawCkUQhWlHoGbwAMLmhprsGOqUBETJZLJeudK4yANYn1ywYgdG2MQLMcVwOD+UGxHjjAMFL/KyRSZmwgn1YErqC4/vVdTMnWjly8+TCtdElZeA/osml1nQnW4bZPNxfRVkVfRqVTIr2pb7miN0FrZNIUdzEjh4ORfaGMGEWxP5NHJ5FkjqIUKP9SmqztQJU/mlpPMX3Pnp897cvQBP5KR7gMWTfkdWYJYEHhyvgNZujQoLl33W4EvbV'
bucket_name = 'desafio-sprint-5-sarah'
file_path = 'C:/Users/Usuario/Desktop/compassacademy/Sprint-5/Desafio/perfil_investidores.csv'
object_name = 'perfil_investidores.csv'


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
   
