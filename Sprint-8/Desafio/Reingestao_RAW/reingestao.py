import tmdbv3api
import datetime
import csv
import json
import sys 
import os
import concurrent.futures
import boto3
import concurrent.futures
import pandas as pd
import io
import requests

tmdb = tmdbv3api.TMDb() 
tmdb.api_key =  'Inserindo a chave da API'
filmes = tmdbv3api.Movie() 
genero = 10752
max_results = 5000
ano_inicial = 2000
ano_final = 2020
discover = tmdbv3api.Discover()
TMDB_AUTH_TOKEN = 'Inserindo token da API'

def serialize_asobj(obj): 
  if hasattr(obj, '__dict__'):
    return {key: value for key, value in vars(obj).items() if isinstance(value, (str, float, int, list, dict, bool))}
  else:
    return
  
def guerra_20anos(filmes, genero_id, max_results, ano_inicial=None, ano_final=None):
    resultados = []
    page = 1
    while len(resultados) < max_results:
        resposta = discover.discover_movies({
            'page': page,
            'with_genres': genero_id,
            'primary_release_date.gte': f"{ano_inicial}-01-01",
            'primary_release_date.lte': f"{ano_final}-12-31"
        })
        if not resposta:
            break
        resultados.extend(resposta)
        page += 1
        if page > 500:
            break
    return resultados[:max_results]


def detalhes(filmes, filme_id, is_movie=True):
    try:
        detalhes = filmes.details(filme_id)
        creditos = filmes.credits(filme_id)
        return {
            'id': detalhes.get('id'),
            'Gêneros': [genre['name'] for genre in detalhes.get('genres', [])],
            'Titulo': detalhes.get('title') if is_movie else detalhes.get('name'),
            'Lancamento': detalhes.get('release_date') if is_movie else detalhes.get('first_air_date'),
            'Visão Geral': detalhes.get('overview', ''),
            'Votos': detalhes.get('vote_count', 0),
            'Média de Votos': detalhes.get('vote_average', 0.0),
            'budget': detalhes.get('budget'),
            'revenue': detalhes.get('revenue'),
            'tagline': detalhes.get('tagline'),
            'duracao': detalhes.get('runtime'),
            'imdb_id': detalhes.get('imdb_id')
        }
    except Exception as e:
        print(f"Erro ao obter detalhes do filme {filme_id}: {e}")
        return None  


def processamento(filme_ids):
  with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    return list(executor.map(lambda filme_id: detalhes(filmes, filme_id), filme_ids))


def lambda_handler(event, context):
  resultados = guerra_20anos(filmes, genero, max_results, ano_inicial, ano_final)
  bucket_name = "desafio-final-sarah"
  s3= boto3.client('s3')
  file_name = "/tmp/detalhes_filmes.json"
  with open(file_name, 'w', encoding='utf-8') as f:
    json.dump([serialize_asobj(filme) for filme in resultados], f, ensure_ascii=False, indent=4)
  
  s3.upload_file(file_name, bucket_name, "arquivos_temporarios/detalhes_filmes.json")
  file_output = "arquivos_temporarios/detalhes_filmes.json"
  obj = s3.get_object(Bucket=bucket_name, Key=file_output)
  file_content = obj["Body"].read()
  df1 = pd.read_csv(io.BytesIO(file_content))
  if "id" not in df1.columns:
    return {
    "statusCode": 400,
    "body": "Erro: O arquivo não contém a coluna 'id'."
    }
  ids = df1["id"].tolist() 
  headers = {
     "accept": "application/json",
    "Authorization": f"Bearer {TMDB_AUTH_TOKEN}"
    }
  movie_data = []
  for id in ids:
    url = f"https://api.themoviedb.org/3/movie/{id}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
         data = response.json()
         movie_data.append({
                "id": data.get("id"),
                "title": data.get("title"),
                "release_date": data.get("release_date"),
                "overview": data.get("overview"),
                "vote_average": data.get("vote_average"),
                "belongs_to_collection": data["belongs_to_collection"]["name"] if data.get("belongs_to_collection") else None,
                "genres": [genre.get("name") for genre in data.get("genres", [])],  # Extrai os nomes dos gêneros
                "budget": data.get("budget"),
                "revenue": data.get("revenue"),
                "original_language": data.get("original_language"),
                "original_title": data.get("original_title"),
                "popularity": data.get("popularity"),
                "runtime": data.get("runtime"),
                "status": data.get("status"),
                "tagline": data.get("tagline"),
                "vote_count": data.get("vote_count"),
                "production_companies": [company.get("name") for company in data.get("production_companies", [])],  # Extrai os nomes das empresas produtoras
                "production_countries": [country.get("name") for country in data.get("production_countries", [])],  # Extrai os nomes dos países de produção
                "runtime": data.get("runtime"),
                "spoken_languages": [language.get("name") for language in data.get("spoken_languages", [])],  # Extrai os nomes das línguas faladas
                "tagline": data.get("tagline")
         })
    else:
        print(f"Erro ao buscar ID {id}: {response.status_code}")


    results_df = pd.DataFrame(movie_data)
    output_buffer = io.StringIO()
    results_df.to_csv(output_buffer, index=False)
    output_key = "arquivos_temporarios/detalhes.csv"
    s3.put_object(Bucket=bucket_name, Key=output_key, Body=output_buffer.getvalue())
    resposta = s3.get_object(Bucket=bucket_name, Key=output_key)
    file_content1 = resposta["Body"].read().decode("utf-8")
    df2 = pd.read_csv(io.StringIO(file_content1))
    now = datetime.datetime.now()
    ano = now.strftime('%Y')
    mes = now.strftime('%m')
    dia = now.strftime('%d')

    for i in range(0, len(df2), 100):  
        batch_df = df2[i:i + 100]
        json_data = batch_df.to_dict(orient='records')
        json_string = json.dumps(json_data, ensure_ascii=False, indent=4)

        output_key1 = f"Raw/TMDB/JSON/{ano}/{mes}/{dia}/filmes_{i // 100 + 1}.json"
        s3.put_object(
            Bucket=bucket_name,
            Key=output_key1,
            Body=json_string.encode('utf-8')
        )

    s3.delete_object(Bucket=bucket_name, Key=output_key)

    reviews = []

    for id in ids:
       url1 = f"https://api.themoviedb.org/3/movie/{id}/reviews?language=en-US&page=1"
       response1 = requests.get(url1, headers=headers)
       if response.status_code == 200:
          data1 = response1.json()
          reviews.append({
            "id": id,
            "reviews": data.get("results", []) })
          
    results_df1 = pd.DataFrame(reviews)
    output_buffer = io.StringIO()
    results_df1.to_csv(output_buffer, index=False)
    output_key2 = "arquivos_temporarios/review.csv"
    s3.put_object(Bucket=bucket_name, Key=output_key2, Body=output_buffer.getvalue())
    resposta1 = s3.get_object(Bucket=bucket_name, Key=output_key2)
    file_content2 = resposta1["Body"].read().decode("utf-8")
    df3 = pd.read_csv(io.StringIO(file_content2))
    for i in range(0, len (df3), 100):
        batch_df1 = df3[i:i + 100]
        json_data1 = batch_df1.to_dict(orient='records')
        json_string1 = json.dumps(json_data1, ensure_ascii=False, indent=4)
        output_key3 = f"Raw/TMDB/JSON/{ano}/{mes}/{dia}/reviews_{i // 100 + 1}.json"
        s3.put_object(
            Bucket=bucket_name,
            Key=output_key3,
            Body=json_string1.encode('utf-8')
        )
    
    provider = []
    for id in ids:
        url2 = f"https://api.themoviedb.org/3/movie/{id}/watch/providers"
        response2 = requests.get(url2, headers=headers)
        if response.status_code == 200:
            data2 = response2.json()
            provider.append({
                "id": id,
                "provider": data.get("results", [])})
            
    results_df2 = pd.DataFrame(provider)
    output_buffer1 = io.StringIO()
    results_df2.to_csv(output_buffer1, index=False)
    output_key4 = "arquivos_temporarios/provider.csv"
    s3.put_object(Bucket=bucket_name, Key=output_key4, Body=output_buffer1.getvalue())
    resposta2 = s3.get_object(Bucket=bucket_name, Key=output_key4)
    file_content3 = resposta2["Body"].read().decode("utf-8")
    df4 = pd.read_csv(io.StringIO(file_content3))
    for i in range(0, len(df4), 100):
        batch_df2 = df4[i:i + 100]
        json_data2 = batch_df2.to_dict(orient='records')
        json_string2 = json.dumps(json_data2, ensure_ascii=False, indent=4)
        output_key5 = f"Raw/TMDB/JSON/{ano}/{mes}/{dia}/provider_{i // 100 + 1}.json"
        s3.put_object(
            Bucket=bucket_name,
            Key=output_key5,
            Body=json_string2.encode('utf-8')
        )
    s3.delete_object(Bucket=bucket_name, Key=output_key2)
    s3.delete_object(Bucket=bucket_name, Key=output_key4)
    s3.delete_object(Bucket=bucket_name, Key=file_output)
           

  return {
    'statusCode': 200,
    'body': json.dumps({"dados salvos com sucesso"})
  }