# Script para gerar a dimensão de produtoras a partir do dataset de filmes

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
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_TARGET_PATH', 'S3_INPUT_PATH'])

# Inicialização do SparkContext e do GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Carregamento do dataset de filmes
arquivo = args ['S3_INPUT_PATH']

# Leitura do dataset
dados = spark.read.parquet(arquivo)

# Seleção da coluna de produtoras
production_companies = dados.select("production_companies").distinct()

# Criação de um dataframe pandas para manipulação dos dados
df_production_companies = production_companies.toPandas()

# Limpeza e tratamento dos dados
df_production_companies['production_companies'] = df_production_companies['production_companies'].astype(str)
df_production_companies['production_companies'] = df_production_companies['production_companies'].str.replace('[', '', regex=False)
df_production_companies['production_companies'] = df_production_companies['production_companies'].str.replace(']', '', regex=False)
df_production_companies['production_companies_split'] = df_production_companies['production_companies'].str.split(',')


# Separação das produtoras em colunas
for index, row in df_production_companies.iterrows():
    company_list = row['production_companies_split']
    for i, company in enumerate(company_list):
        if i < 12:
          column_name = f"company_{i+1}"
          df_production_companies.loc[index, column_name] = company.strip()

# Selecionando as produtoras únicas
unique_company_1_values = df_production_companies['company_1'].unique()
unique_company_2_values = df_production_companies['company_2'].unique()
unique_company_3_values = df_production_companies['company_3'].unique()
unique_company_4_values = df_production_companies['company_4'].unique()
unique_company_5_values = df_production_companies['company_5'].unique()
unique_company_6_values = df_production_companies['company_6'].unique()
unique_company_7_values = df_production_companies['company_7'].unique()
unique_company_8_values = df_production_companies['company_8'].unique()
unique_company_9_values = df_production_companies['company_9'].unique()
unique_company_10_values = df_production_companies['company_10'].unique()
unique_company_11_values = df_production_companies['company_11'].unique()
unique_company_12_values = df_production_companies['company_12'].unique()

# Criação da lista de produtoras
produtoras = []
for col in ['company_1', 'company_2', 'company_3', 'company_4', 'company_5', 'company_6', 'company_7', 'company_8', 'company_9', 'company_10', 'company_11', 'company_12']:
    produtoras.extend(df_production_companies[col].unique())
produtoras = [x for x in produtoras if x is not None]
produtoras = list(set(produtoras))

# Removendo valores nulos
produtoras = [produtora for produtora in produtoras if produtora != '']

# Criação da lista de ids
id_prod = list(range(1,758))

# Criação do dataframe de produtoras
df_produtoras = pd.DataFrame({'id_prod': id_prod[:len(produtoras)], 'produtoras': produtoras})

# Criação do dataframe spark
df_produtoras_spark = spark.createDataFrame(df_produtoras)

# Definindo o caminho de saída
output = args['S3_TARGET_PATH']

# Escrita do dataframe no S3
df1 = df_produtoras_spark
df_limited = DynamicFrame.fromDF(df1.limit(100000), glueContext, "df_limited")
glueContext.write_dynamic_frame.from_options(
        frame= df_limited,
        connection_type='s3',
        connection_options={"path": output},
        format="parquet"
        )

job.commit()