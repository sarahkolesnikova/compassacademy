# Script para criação da tabela fato_filmes


# Importação de bibliotecas
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, round, when, regexp_replace, split, to_date
from pyspark.sql.types import IntegerType
import pandas as pd
from functools import reduce
from pyspark.sql import DataFrame
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_TARGET_PATH', 'S3_INPUT_PATH', 'S3_INPUT_PATH1'])

## Inicialização do SparkContext e do GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## Carregamento dos dados sobre filmes
arquivo = args ['S3_INPUT_PATH']

## Leitura dos dados
dados = spark.read.parquet(arquivo)

## Leitura dos dados de tempo
arquivo1 = args ['S3_INPUT_PATH1']

## Leitura dos dados
tempo = spark.read.parquet(arquivo1)

## Transformações dos dados sobre filmes
dados = dados.withColumn("datalancamento", to_date(col("datalancamento"), "yyyy-MM-dd"))

# Fazendo o join dos dados de filmes com os dados de tempo para obter o id_tempo
df_left = dados.join(tempo, on="datalancamento", how="left")

# Selecionando as colunas necessárias para a tabela fato_filmes
fato_filmes = df_left.select("id", "vote_average", "budget", "revenue", "popularity", "vote_count", "id_guerra", "id_tempo")


# Escrevendo a fato_filmes no S3
output = args['S3_TARGET_PATH']
df1 = fato_filmes
df_limited = DynamicFrame.fromDF(df1.limit(100000), glueContext, "df_limited")
glueContext.write_dynamic_frame.from_options(
        frame= df_limited,
        connection_type='s3',
        connection_options={"path": output},
        format="parquet"
        )

job.commit()