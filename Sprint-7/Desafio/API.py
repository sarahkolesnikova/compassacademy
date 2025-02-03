import datetime # Importando a biblioteca datetime
import tmdbv3api # Importando a biblioteca tmdbv3api
import json # Importando a biblioteca json
import boto3 # Importando a biblioteca boto3


tmdb = tmdbv3api.TMDb() # Instanciando a classe TMDb
tmdb.api_key = 'INSERIR CHAVE AQUI' # Inserindo a chave da API
filmes = tmdbv3api.Movie() # Instanciando a classe Movie

def serialize_asobj(obj): # Função para serializar o objeto
  if hasattr(obj, '__dict__'):
    return {key: value for key, value in vars(obj).items() if isinstance(value, (str, float, int, list, dict, bool))}
  else:
    return

def detalhes (filmes, item_id, is_movie = True): # Função para detalhar os filmes
  detalhes = filmes.details(item_id)
  creditos = filmes.credits(item_id)
  cast_lista = list(creditos.cast)[:10]

  return {
    'id': detalhes['id'],
    'Gêneros': [genre['name'] for genre in detalhes['genres']],
    'Titulo': detalhes['title'] if is_movie else detalhes['name'],
    'Lancamento': detalhes['release_date'] if is_movie else detalhes['first_air_date'],
    'Visão Geral': detalhes['overview'],
    'Votos': detalhes['vote_count'],
    'Média de Votos': detalhes['vote_average'],
    "budget": detalhes['budget'] if is_movie else None,
    "VisaoGeral": detalhes['overview'],
    "revenue": detalhes['revenue'] if is_movie else None,
    "tagline": detalhes['tagline'] if is_movie else None,
    'duracao': detalhes['runtime'] if is_movie else None,
    "imdb_id": detalhes['imdb_id'] if is_movie else None

  }

def filmes_filtrados(filmes, genero_id, max_results, ano_inicial=None, ano_final=None): # Função para filtrar os filmes
  discover = tmdbv3api.Discover()
  resultados = []
  page = 1
  while len(resultados) < max_results:
    resposta = discover.discover_movies({'page': page, 'with_genres': genero_id}) 
    if not resposta:
      break
    filtros = [
      item for item in resposta
      if hasattr(item, 'genre_ids') and genero_id in item.genre_ids
    ]
    if ano_inicial or ano_final:
      filtros = [
        item for item in filtros
        if hasattr(item, 'release_date') and 
        (ano_inicial is None or item.release_date >= ano_inicial) and 
        (ano_final is None or item.release_date <= ano_final)
      ]
    resultados.extend(filtros)
    page += 1
    if page > 500:
      break
  return resultados[:max_results]

def lambda_handler(event, context): # Função principal
  filmes_guerra = filmes_filtrados(filmes, 10752, 5000, "2000-01-01", "2020-12-31")
  s3 = boto3.client('s3')
  bucket_name = 'desafio-final-sarah'
  now = datetime.datetime.now()
  ano = now.strftime('%Y')
  mes = now.strftime('%m')
  dia = now.strftime('%d')
  for i in range(0, len(filmes_guerra), 100): # Loop para salvar os filmes no S3
    s3_path = f'Raw/TMDB/JSON/{ano}/{mes}/{dia}/filmes_guerra_{i//100+1}.json'
    filmes_guerra_serialized = [serialize_asobj(filme) for filme in filmes_guerra[i:i+100]]
    s3.put_object(Bucket=bucket_name, Key=s3_path, Body=json.dumps(filmes_guerra_serialized))

  return {
    'statusCode': 200,
    'body': json.dumps({"dados salvos com sucesso": f"s3://{bucket_name}"})
  }



