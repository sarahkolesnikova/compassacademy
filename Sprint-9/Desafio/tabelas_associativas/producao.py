# Script para realizar a associação entre os filmes e as produtoras

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

## Inicialização do SparkContext e do GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## Carregamento dos dados
arquivo = args ['S3_INPUT_PATH']
dados = spark.read.parquet(arquivo)

# Selecionando as colunas necessárias
production_companies = dados.select("id", "production_companies")

# Transformando a coluna production_companies em um DataFrame Pandas
df_production_companies = production_companies.toPandas()

# Tratamento da coluna production_companies
df_production_companies['production_companies'] = df_production_companies['production_companies'].astype(str)
df_production_companies['production_companies'] = df_production_companies['production_companies'].str.replace('[', '', regex=False)
df_production_companies['production_companies'] = df_production_companies['production_companies'].str.replace(']', '', regex=False)
df_production_companies['production_companies_split'] = df_production_companies['production_companies'].str.split(',')

# Criação de novas colunas para cada produtora
for index, row in df_production_companies.iterrows():
    company_list = row['production_companies_split']
    for i, company in enumerate(company_list):
        if i < 12:
          column_name = f"company_{i+1}"
          df_production_companies.loc[index, column_name] = company.strip()
          
# Criação de um novo DataFrame spark
df_production_companies = spark.createDataFrame(df_production_companies)

# Selecionando as colunas necessárias para a associação, retirando os valores nulos e renomeando as colunas
producao1 = df_production_companies.select("id", "company_1")
producao1 = producao1.na.drop(subset=["company_1"])
producao1 = producao1.withColumnRenamed("company_1", "produtoras")

producao2 = df_production_companies.select("id", "company_2")
producao2 = producao2.na.drop(subset=["company_2"])
producao2 = producao2.withColumnRenamed("company_2", "produtoras")

producao3 = df_production_companies.select("id", "company_3")
producao3 = producao3.na.drop(subset=["company_3"])
producao3 = producao3.withColumnRenamed("company_3", "produtoras")

producao4 = df_production_companies.select("id", "company_4")
producao4 = producao4.na.drop(subset=["company_4"])
producao4 = producao4.withColumnRenamed("company_4", "produtoras")

producao5 = df_production_companies.select("id", "company_5")
producao5 = producao5.na.drop(subset=["company_5"])
producao5 = producao5.withColumnRenamed("company_5", "produtoras")

producao6 = df_production_companies.select("id", "company_6")
producao6 = producao6.na.drop(subset=["company_6"])
producao6 = producao6.withColumnRenamed("company_6", "produtoras")

producao7 = df_production_companies.select("id", "company_7")
producao7 = producao7.na.drop(subset=["company_7"])
producao7 = producao7.withColumnRenamed("company_7", "produtoras")

producao8 = df_production_companies.select("id", "company_8")
producao8 = producao8.na.drop(subset=["company_8"])
producao8 = producao8.withColumnRenamed("company_8", "produtoras")

producao9 = df_production_companies.select("id", "company_9")
producao9 = producao9.na.drop(subset=["company_9"])
producao9 = producao9.withColumnRenamed("company_9", "produtoras")

producao10 = df_production_companies.select("id", "company_10")
producao10 = producao10.na.drop(subset=["company_10"])
producao10 = producao10.withColumnRenamed("company_10", "produtoras")

producao11 = df_production_companies.select("id", "company_11")
producao11 = producao11.na.drop(subset=["company_11"])
producao11 = producao11.withColumnRenamed("company_11", "produtoras")

producao12 = df_production_companies.select("id", "company_12")
producao12 = producao12.na.drop(subset=["company_12"])
producao12 = producao12.withColumnRenamed("company_12", "produtoras")

# Realizando a união dos DataFrames
assoc_producao = producao1.union(producao2)
assoc_producao = assoc_producao.union(producao3)
assoc_producao = assoc_producao.union(producao4)
assoc_producao = assoc_producao.union(producao5)
assoc_producao = assoc_producao.union(producao6)
assoc_producao = assoc_producao.union(producao7)
assoc_producao = assoc_producao.union(producao8)
assoc_producao = assoc_producao.union(producao9)
assoc_producao = assoc_producao.union(producao10)
assoc_producao = assoc_producao.union(producao11)
assoc_producao = assoc_producao.union(producao12)

# Carregamento dos dados da dimensão produtoras
arquivo1 = args ['S3_INPUT_PATH1']
production_companies = spark.read.parquet(arquivo1)

# Realizando a associação entre os ids dos filmes e os ids das produtoras
df_left = assoc_producao.join(production_companies, on= "produtoras", how="left")

# Selecionando as colunas necessárias para a tabela de produção
produzir = df_left.select("id", "id_prod")

# Escrevendo os dados no S3
output = args['S3_TARGET_PATH']
df1 = produzir
df_limited = DynamicFrame.fromDF(df1.limit(100000), glueContext, "df_limited")
glueContext.write_dynamic_frame.from_options(
        frame= df_limited,
        connection_type='s3',
        connection_options={"path": output},
        format="parquet"
        )


job.commit()