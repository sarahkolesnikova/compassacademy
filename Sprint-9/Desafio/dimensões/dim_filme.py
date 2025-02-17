
# importando bibliotecas
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, round, when, regexp_replace, split, lit
from pyspark.sql.types import IntegerType
import pandas as pd
from functools import reduce
from pyspark.sql import DataFrame
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_TARGET_PATH', 'S3_TARGET_PATH1', 'S3_INPUT_PATH', 'S3_INPUT_PATH1'])


# Inicializando o SparkContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

arquivo = args ['S3_INPUT_PATH'] # arquivo de entrada "detalhes" da trusted
detalhes =  spark.read.parquet(arquivo) # lendo o arquivo

# Tratamento dos dados
detalhes = detalhes.withColumn("dataLancamento", split(col("release_date").getField("string"), ",").getItem(0))
detalhes = detalhes.drop("release_date")
detalhes = detalhes.withColumn("ano", split(col("dataLancamento"), "-").getItem(0))
detalhes = detalhes.withColumn("mes", split(col("dataLancamento"), "-").getItem(1))
detalhes = detalhes.withColumn("ano", col("ano").cast(IntegerType()))
detalhes = detalhes.withColumn("overviews", split(col("overview").getField("string"), ",").getItem(0))
detalhes = detalhes.drop("overview")
detalhes = detalhes.drop("belongs_to_collection")
detalhes = detalhes.drop("tagline")
detalhes = detalhes.drop("status")

# Filtrando os dados para os filmes de guerra produzidos entre 2000 e 2020
detalhes_filtrados = detalhes.filter((col("ano") >= 2000) & (col("ano") <= 2020))

# Filtrando os filmes da segunda guerra mundial
palavras_chave = ["nazi", "Nazi", "Nazism", "nazism", "dunkirk", "Dunkirk", "stalingrad", "Stalingrad", "eastern front", "Eastern Front",
                  "western front", "front", "Front", "Western Front","camps", "concentration camps", "genocide" ,"Pearl Harbor", "El Alamein", "Normandy",
                  "blitzkrieg", "Shoah", "Reconstruction", "denazification", "Nuremberg", "Resistance", "Invasion",
                  "German", "Jewish", "german", "jewish", "Germany", "fascism", "second war", "II war", "1935", "1936", "1937", "1938", "1939",
                  "1940", "1941", "1942", "1943", "1944", "1945","dominance", "collaboration", "Second World War", "second world war",
                  "Allied Powers", "Axis Powers", "allied", "allied powers", "axis powers",
                  "WWII", "wwII", "Holocaust", "holocaust","evacuation", "ghetto", "World War II", "world war II", "Second War", "second war", "Hitler",
                  "hitler", "Mussolini", "mussolini", "Churchill", "churchill", "Hiroshima", "hiroshima", "Nagasaki", "nagasaki", "D-Day", "d-day","Operation Netptuno","operation neptuno"]
detalhes_filtrados1 = detalhes_filtrados.filter(
    col("overviews").rlike("|".join(palavras_chave))
)
print(detalhes_filtrados1.count())

# Filtrando os filmes da primeira guerra mundial
palavras_chave1 = ["Trench warfare", "trench warfare", "Shell shock", "shell shock", "Mustard gas", "mustard gas", "Dreadnought",
                   "dreadnought", "Treaty of Versailles", "treaty of versailles", "Lost Generation", "lost generation",
                   "WWI", "wwi", "1914", "1915", "1916", "1917", "1918", "Great War", "great war", "First World War", "first world war",
                   "first war", "world war I", "World War I", "Ottoman", "ottoman","Ottoman Empire", "ottoman empire", "Turk Empire",
                   "turk empire", "armistice","Armistice", "Imperialism", "imperialism", "austro-hungarian","Austro-Hungarian", "Ships", "ships"
                   "Cavalry", "cavalry","Trench", "trench", "Triple Entente", "triple entente", "Central Powers", "central powers"]
detalhes_filtrados2 = detalhes_filtrados.filter(
    col("overviews").rlike("|".join(palavras_chave1))
)
print(detalhes_filtrados2.count())

# Filtrando os filmes da guerra do Iraque
palavras_chave2 = ["Iraq War", "Iraq", "iraq war", "Operation Iraqi Freedom", "operation iraqi freedom",
                   "Coalition of the Willing", "coalition of the willing", "Saddam Hussein", "saddam hussein",
                   "Sunni", "sunni", "Shia", "shia", "Gulf War", "gulf war", "Operation Desert Shield", "operation desert shield"
                   "Kuwait", "Persian Gulf", "kuwait", "persian gulf"]

detalhes_filtrados3 = detalhes_filtrados.filter(
    col("overviews").rlike("|".join(palavras_chave2))
)
print(detalhes_filtrados3.count())

# Filtrando os filmes da guerra do Afeganistão
palavras_chave3 = ["War in Afghanistan", "Afghanistan", "afghanistan", "Afghan War", "afghan war", "Afghan", "afghan",
                   "Global War on Terror", "global war on terror", "Taliban", "taliban", "Al-Qaeda", "al-qaeda", "US-led coalition",
                   "Warlord", "warlord", "Opium poppy", "opium poppy", "Madrasa", "madrasa", "withdrawal", "Refugee crisis", "refugee crisis", "Terrorism", "terrorism"]
detalhes_filtrados4 = detalhes_filtrados.filter(
    col("overviews").rlike("|".join(palavras_chave3))
)
print(detalhes_filtrados4.count())


arquivo1 = args ['S3_INPUT_PATH1'] # arquivo de entrada da "reviews" da camada trusted
reviews = spark.read.parquet(arquivo1) # lendo arquivo

# Tratamento dos dados
review = reviews.withColumn("content", split(col("reviews"), ",").getItem(5))
review = review.drop("reviews")

# Criando uma lista com os ids dos filmes filtrados pelas guerras
id_2_guerra = detalhes_filtrados1.select("id").toPandas()["id"].tolist()
id_1_guerra = detalhes_filtrados2.select("id").toPandas()["id"].tolist()
id_iraque = detalhes_filtrados3.select("id").toPandas()["id"].tolist()
id_afegan = detalhes_filtrados4.select("id").toPandas()["id"].tolist()
lista_ids = id_2_guerra + id_1_guerra + id_iraque + id_afegan
lista_ids_unique = list(set(lista_ids))
df_ids_unique = pd.DataFrame({'id': lista_ids_unique})
id_list = df_ids_unique['id'].tolist()

# Criando dataframe com os detalhes dos filmes filtrados pelas guerras
filtered_reviews = review.filter(col("id").isin(id_list))
dfs = [detalhes_filtrados1, detalhes_filtrados2, detalhes_filtrados3, detalhes_filtrados4]
detalhes_filtrados_combined = reduce(DataFrame.unionAll, dfs)
detalhes_filtrados_combined = detalhes_filtrados_combined.dropDuplicates()
detalhes_filtrados_combined = detalhes_filtrados_combined.join(filtered_reviews, on="id", how="left") # join com as reviews
detalhes_filtrados_combined = detalhes_filtrados_combined.withColumnRenamed("content", "reviews") # renomeando a coluna
intersecao = detalhes_filtrados1.intersect(detalhes_filtrados2) # interseção entre os filmes da primeira e segunda guerra
id_1e2_guerra = intersecao.select("id").toPandas()["id"].tolist() # lista com os ids dos filmes da primeira e segunda guerra
id_2_guerra = [id for id in id_2_guerra if id != 100540] # removendo o id 100540
detalhes_filtrados_combined = detalhes_filtrados_combined.withColumn("id_guerra", lit(0)) # criando a coluna id_guerra

# Atribuindo um valor para a coluna id_guerra
detalhes_filtrados_combined = detalhes_filtrados_combined.withColumn(
    "id_guerra",
    when(col("id").isin(id_2_guerra), 2).otherwise(col("id_guerra"))
)
detalhes_filtrados_combined = detalhes_filtrados_combined.withColumn(
    "id_guerra",
    when(col("id").isin(id_1_guerra), 1).otherwise(col("id_guerra"))
)
detalhes_filtrados_combined = detalhes_filtrados_combined.withColumn(
    "id_guerra",
    when(col("id").isin(id_iraque), 3).otherwise(col("id_guerra"))
)
detalhes_filtrados_combined = detalhes_filtrados_combined.withColumn(
    "id_guerra",
    when(col("id").isin(id_afegan), 4).otherwise(col("id_guerra"))
)
detalhes_filtrados_combined = detalhes_filtrados_combined.withColumn(
    "id_guerra",
    when(col("id").isin(id_1e2_guerra), 5).otherwise(col("id_guerra"))
)

# Escrevendo os dados em uma tabela temporaria no S3
output = args['S3_TARGET_PATH'] # caminho de saída
df1 = detalhes_filtrados_combined # dataframe com os detalhes dos filmes filtrados pelas guerras

df_limited = DynamicFrame.fromDF(df1.limit(100000), glueContext, "df_limited") # limitando o número de linhas

glueContext.write_dynamic_frame.from_options( 
        frame= df_limited,
        connection_type='s3',
        connection_options={"path": output},
        format="parquet"
        )

# Criando a dimensão dim_filme
dim_filme = detalhes_filtrados_combined.drop("vote_average", "genres", "budget","revenue",
                                             "popularity","vote_count", "production_companies", "spoken_languages", 
                                             "production_countries", "ano", "belongs_to_collection_string", "mes", "dataLancamento", "id_guerra")
dim_filme = dim_filme.withColumnRenamed("id", "id_tmdb")

# Escrevendo os dados na tabela dim_filme no S3

output1 = args['S3_TARGET_PATH1'] # caminho de saída
df2 = dim_filme

df_limited1 = DynamicFrame.fromDF(df2.limit(100000), glueContext, "df_limited1") # limitando o número de linhas
glueContext.write_dynamic_frame.from_options(
        frame= df_limited1,
        connection_type='s3',
        connection_options={"path": output1},
        format="parquet"
        )

job.commit()