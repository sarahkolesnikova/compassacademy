# Script para criar a dim_tempo no Glue

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
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_TARGET_PATH', 'S3_INPUT_PATH'])

# iniciando o spark context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Definindo o caminho de entrada
arquivo = args ['S3_INPUT_PATH']

# Carregando os dados
dados = spark.read.parquet(arquivo)

# Selecionando as colunas necessárias
lancamento = dados.select("datalancamento", "ano", "mes")
print(lancamento.count())

# Removendo duplicados
lancamento = lancamento.dropDuplicates(["datalancamento"])
print(lancamento.count())

# transformando a coluna datalancamento para o formato date
lancamento = lancamento.withColumn("datalancamento", to_date(col("datalancamento"), "yyyy-MM-dd"))

# Ordenando os dados
lancamento = lancamento.orderBy("datalancamento")

# Criando o id_tempo
id_lista_tempo = list(range(1,362))

# Criando o dataframe
df_tempo = pd.DataFrame({'id_tempo': id_lista_tempo[:lancamento.count()], 
                               'dataLancamento': lancamento.toPandas()['datalancamento'].tolist(),
                               'ano': lancamento.toPandas()['ano'].tolist(), 
                               'mes': lancamento.toPandas()['mes'].tolist()})

# Criando o dataframe no spark                  
df_tempo_spark = spark.createDataFrame(df_tempo)

# Definindo o caminho de saída
output = args['S3_TARGET_PATH']

# Escrevendo o dataframe no S3
df1 = df_tempo_spark
df_limited = DynamicFrame.fromDF(df1.limit(100000), glueContext, "df_limited")
glueContext.write_dynamic_frame.from_options(
        frame= df_limited,
        connection_type='s3',
        connection_options={"path": output},
        format="parquet"
        )
job.commit()