# Script para gerar a dimensão de idiomas

# Importação de bibliotecas
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, round, when, regexp_replace, split
from pyspark.sql.types import IntegerType
import pandas as pd
from functools import reduce
from pyspark.sql import DataFrame
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_TARGET_PATH', 'S3_INPUT_PATH'])

# iniciando o spark context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Definindo o caminho de entrada
arquivo = args ['S3_INPUT_PATH']

# Lendo os dados
dados = spark.read.parquet(arquivo)

# Selecionando a coluna de idiomas do arquivo temporario
idiomas = dados.select("spoken_languages")
unique_idiomas = idiomas.select("spoken_languages").distinct()

# Separando os idiomas em colunas
unique_idiomas = unique_idiomas.withColumn("idioma1", split(col("spoken_languages"), ",").getItem(0))
unique_idiomas = unique_idiomas.withColumn("idioma2", split(col("spoken_languages"), ",").getItem(1)) 
unique_idiomas = unique_idiomas.withColumn("idioma3", split(col("spoken_languages"), ",").getItem(2)) 
unique_idiomas = unique_idiomas.withColumn("idioma4", split(col("spoken_languages"), ",").getItem(3)) 
unique_idiomas = unique_idiomas.withColumn("idioma5", split(col("spoken_languages"), ",").getItem(4)) 
unique_idiomas = unique_idiomas.withColumn("idioma6", split(col("spoken_languages"), ",").getItem(5)) 
unique_idiomas = unique_idiomas.withColumn("idioma7", split(col("spoken_languages"), ",").getItem(6)) 
unique_idiomas = unique_idiomas.withColumn("idioma8", split(col("spoken_languages"), ",").getItem(7)) 

# Convertendo para pandas
df_unique_idiomas = unique_idiomas.toPandas()

# Tratando os dados
df_unique_idiomas['idioma1'] = df_unique_idiomas['idioma1'].str.replace('[', '', regex=False)
df_unique_idiomas['idioma1'] = df_unique_idiomas['idioma1'].str.replace(']', '', regex=False)
df_unique_idiomas['idioma2'] = df_unique_idiomas['idioma2'].str.replace('[', '', regex=False)
df_unique_idiomas['idioma2'] = df_unique_idiomas['idioma2'].str.replace(']', '', regex=False)
df_unique_idiomas['idioma3'] = df_unique_idiomas['idioma3'].str.replace('[', '', regex=False)
df_unique_idiomas['idioma3'] = df_unique_idiomas['idioma3'].str.replace(']', '', regex=False)
df_unique_idiomas['idioma4'] = df_unique_idiomas['idioma4'].str.replace('[', '', regex=False)
df_unique_idiomas['idioma4'] = df_unique_idiomas['idioma4'].str.replace(']', '', regex=False)
df_unique_idiomas['idioma5'] = df_unique_idiomas['idioma5'].str.replace('[', '', regex=False)
df_unique_idiomas['idioma5'] = df_unique_idiomas['idioma5'].str.replace(']', '', regex=False)
df_unique_idiomas['idioma6'] = df_unique_idiomas['idioma6'].str.replace('[', '', regex=False)
df_unique_idiomas['idioma6'] = df_unique_idiomas['idioma6'].str.replace(']', '', regex=False)
df_unique_idiomas['idioma7'] = df_unique_idiomas['idioma7'].str.replace('[', '', regex=False)
df_unique_idiomas['idioma7'] = df_unique_idiomas['idioma7'].str.replace(']', '', regex=False)
df_unique_idiomas['idioma8'] = df_unique_idiomas['idioma8'].str.replace('[', '', regex=False)
df_unique_idiomas['idioma8'] = df_unique_idiomas['idioma8'].str.replace(']', '', regex=False)

# Selecionando os idiomas únicos
idiomas_values1 = df_unique_idiomas['idioma1'].unique()
idiomas_values2 = df_unique_idiomas['idioma2'].unique()
idiomas_values3 = df_unique_idiomas['idioma3'].unique()
idiomas_values4 = df_unique_idiomas['idioma4'].unique()
idiomas_values5 = df_unique_idiomas['idioma5'].unique()
idiomas_values6 = df_unique_idiomas['idioma6'].unique()
idiomas_values7 = df_unique_idiomas['idioma7'].unique()
idiomas_values8 = df_unique_idiomas['idioma8'].unique()

# Concatenando os idiomas únicos em uma lista
idiom = []
for col in ['idioma1', 'idioma2', 'idioma3', 'idioma4', 'idioma5', 'idioma6', 'idioma7', 'idioma8']:
  idiom.extend(df_unique_idiomas[col].unique())

idiom = [x for x in idiom if x is not None]
idiom = list(set(idiom))

# Removendo valores nulos
idiom = [idioma for idioma in idiom if idioma != '']
print(idiom)
print(len(idiom)) # contagem de idiomas

# Criando uma lista de id_idioma
id_idioma =list(range(1,76))

# Criando um dataframe com os idiomas
df_idiomas = pd.DataFrame({'id_idioma': id_idioma[:len(idiom)], 'idiomas': idiom})

# Criando um dataframe spark
df_idiomas_spark = spark.createDataFrame(df_idiomas)

# Definindo o caminho de saída
output = args['S3_TARGET_PATH']

# Escrevendo o dataframe no S3
df1 = df_idiomas_spark
df_limited = DynamicFrame.fromDF(df1.limit(100000), glueContext, "df_limited")
glueContext.write_dynamic_frame.from_options(
        frame= df_limited,
        connection_type='s3',
        connection_options={"path": output},
        format="parquet"
        )

job.commit()