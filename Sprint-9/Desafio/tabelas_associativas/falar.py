# Script para realizar a associação entre os filmes e os idiomas falados nos filmes

# Importação das bibliotecas
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
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_TARGET_PATH', 'S3_INPUT_PATH', 'S3_INPUT_PATH1'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Leitura dos dados sobre os filmes e os idiomas falados
arquivo = args ['S3_INPUT_PATH']
dados = spark.read.parquet(arquivo)

# Seleção dos ids dos filmes e dos idiomas falados
idiomas = dados.select("id", "spoken_languages")

# Separação dos idiomas falados em colunas diferentes
idiomas= idiomas.withColumn("idioma1", split(col("spoken_languages"), ",").getItem(0))
idiomas = idiomas.withColumn("idioma2", split(col("spoken_languages"), ",").getItem(1)) 
idiomas = idiomas.withColumn("idioma3", split(col("spoken_languages"), ",").getItem(2)) 
idiomas = idiomas.withColumn("idioma4", split(col("spoken_languages"), ",").getItem(3)) 
idiomas = idiomas.withColumn("idioma5", split(col("spoken_languages"), ",").getItem(4)) 
idiomas = idiomas.withColumn("idioma6", split(col("spoken_languages"), ",").getItem(5)) 
idiomas = idiomas.withColumn("idioma7", split(col("spoken_languages"), ",").getItem(6)) 
idiomas = idiomas.withColumn("idioma8", split(col("spoken_languages"), ",").getItem(7))

# Conversão dos dados para o formato pandas
idiomas_pd = idiomas.toPandas()

# Remoção dos caracteres especiais
idiomas_pd['idioma1'] = idiomas_pd['idioma1'].str.replace('[', '', regex=False)
idiomas_pd['idioma1'] = idiomas_pd['idioma1'].str.replace(']', '', regex=False)
idiomas_pd['idioma2'] = idiomas_pd['idioma2'].str.replace('[', '', regex=False)
idiomas_pd['idioma2'] = idiomas_pd['idioma2'].str.replace(']', '', regex=False)
idiomas_pd['idioma3'] = idiomas_pd['idioma3'].str.replace('[', '', regex=False)
idiomas_pd['idioma3'] = idiomas_pd['idioma3'].str.replace(']', '', regex=False)
idiomas_pd['idioma4'] = idiomas_pd['idioma4'].str.replace('[', '', regex=False)
idiomas_pd['idioma4'] = idiomas_pd['idioma4'].str.replace(']', '', regex=False)
idiomas_pd['idioma5'] = idiomas_pd['idioma5'].str.replace('[', '', regex=False)
idiomas_pd['idioma5'] = idiomas_pd['idioma5'].str.replace(']', '', regex=False)
idiomas_pd['idioma6'] = idiomas_pd['idioma6'].str.replace('[', '', regex=False)
idiomas_pd['idioma6'] = idiomas_pd['idioma6'].str.replace(']', '', regex=False)
idiomas_pd['idioma7'] = idiomas_pd['idioma7'].str.replace('[', '', regex=False)
idiomas_pd['idioma7'] = idiomas_pd['idioma7'].str.replace(']', '', regex=False)
idiomas_pd['idioma8'] = idiomas_pd['idioma8'].str.replace('[', '', regex=False)
idiomas_pd['idioma8'] = idiomas_pd['idioma8'].str.replace(']', '', regex=False)

# Criação do DataFrame spark com os idiomas falados
idiomas_pd = spark.createDataFrame(idiomas_pd)

# Seleção dos idiomas falados
idiomas1 = idiomas_pd.select("id", "idioma1") # Seleção do id e do idioma1
idiomas1 = idiomas1.na.drop(subset=["idioma1"]) # Remoção dos valores nulos
idiomas1 = idiomas1.withColumnRenamed("idioma1", "idiomas") # Renomeação da coluna idioma1 para idiomas

# Repetição do processo para os demais idiomas
idiomas2 = idiomas_pd.select("id", "idioma2")
idiomas2 = idiomas2.na.drop(subset=["idioma2"])
idiomas2 = idiomas2.withColumnRenamed("idioma2", "idiomas")

idiomas3 = idiomas_pd.select("id", "idioma3")
idiomas3 = idiomas3.na.drop(subset=["idioma3"])
idiomas3 = idiomas3.withColumnRenamed("idioma3", "idiomas")

idiomas4 = idiomas_pd.select("id", "idioma4")
idiomas4 = idiomas4.na.drop(subset=["idioma4"])
idiomas4 = idiomas4.withColumnRenamed("idioma4", "idiomas")

idiomas5 = idiomas_pd.select("id", "idioma5")
idiomas5 = idiomas5.na.drop(subset=["idioma5"])
idiomas5 = idiomas5.withColumnRenamed("idioma5", "idiomas")

idiomas6 = idiomas_pd.select("id", "idioma6")
idiomas6 = idiomas6.na.drop(subset=["idioma6"])
idiomas6 = idiomas6.withColumnRenamed("idioma6", "idiomas")

idiomas7 = idiomas_pd.select("id", "idioma7")
idiomas7 = idiomas7.na.drop(subset=["idioma7"])
idiomas7 = idiomas7.withColumnRenamed("idioma7", "idiomas")

idiomas8 = idiomas_pd.select("id", "idioma8")
idiomas8 = idiomas8.na.drop(subset=["idioma8"])
idiomas8 = idiomas8.withColumnRenamed("idioma8", "idiomas")

# União dos DataFrames com os idiomas falados nos filmes
assoc_falar = idiomas1.union(idiomas2)
assoc_falar = assoc_falar.union(idiomas3)
assoc_falar = assoc_falar.union(idiomas4)
assoc_falar = assoc_falar.union(idiomas5)
assoc_falar = assoc_falar.union(idiomas6)
assoc_falar = assoc_falar.union(idiomas7)
assoc_falar = assoc_falar.union(idiomas8)

# Leitura dos dados da tabela de idiomas
arquivo1 = args ['S3_INPUT_PATH1']
speak = spark.read.parquet(arquivo1)

# Associação dos ids de idiomas com os ids dos filmes
df_left = assoc_falar.join(speak, on= "idiomas", how="left")
falar = df_left.select("id", "id_idioma")

# Escrita dos dados no S3
output = args['S3_TARGET_PATH']
df1 = falar
df_limited = DynamicFrame.fromDF(df1.limit(100000), glueContext, "df_limited")
glueContext.write_dynamic_frame.from_options(
        frame= df_limited,
        connection_type='s3',
        connection_options={"path": output},
        format="parquet"
        )

job.commit()