
import requests 
import pandas as pd 
from IPython.display import display 
import hashlib 
import getpass 

chave_API = getpass.getpass('Insira sua chave de API: ') 

url = "https://api.themoviedb.org/3/movie/popular?api_key="+chave_API 

response = requests.get(url) 

data = response.json() 

filmes = [] 
for movie in data['results']: 
    dataframe={'Titulo': movie['title'], 'Data de Lançamento': movie['release_date'], 'Visão Geral': movie['overview'], 
               'Votos': movie['vote_count'], 'Média de Votos': movie['vote_average']} 

filmes.append(dataframe) 

dataframe = pd.DataFrame(filmes) 

display(dataframe)
