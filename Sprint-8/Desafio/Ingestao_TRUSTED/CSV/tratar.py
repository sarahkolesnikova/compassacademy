import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, round, when, regexp_replace, split

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Variáveis
parquet_file_path = args['S3_INPUT_PATH']
bucket_name = 'desafio-final-sarah'
folder_name = "Trusted/Local/Parquet/movies"

# Inicializando o cliente do S3
s3 = boto3.client('s3')

# Lendo o arquivo parquet
movies = movies = spark.read.parquet(parquet_file_path)  

# Tratamento dos dados da coluna 'notaMedia': convertendo para double e arredondando para uma casa decimal
movies = movies.withColumn("notaMedia", round(col("notaMedia").cast("double"), 1))

# Tratamento dos dados da coluna 'numeroVotos': convertendo para inteiro
movies = movies.withColumn("numeroVotos", col("numeroVotos").cast("integer"))

# Tratamento dos dados da coluna 'anoLancamento': convertendo para inteiro
movies = movies.withColumn("anoLancamento", col("anoLancamento").cast("integer"))

# Tratamento dos dados da coluna 'tempoMinutos': removendo caracteres não numéricos e convertendo para inteiro
movies = movies.withColumn("tempoMinutos", when(col("tempoMinutos") == "\\N", "").otherwise(col("tempoMinutos")))
movies = movies.withColumn("tempoMinutos", when(col("tempoMinutos") == "\\N", None).otherwise(col("tempoMinutos")))
movies = movies.withColumn("tempoMinutos", regexp_replace(col("tempoMinutos"), "[^0-9]", ""))
movies = movies.withColumn("tempoMinutos", col("tempoMinutos").cast("int"))

# Ordenando os dados pelas colunas 'anoLancamento', 'notaMedia' e 'numeroVotos'
movies = movies.orderBy(col("anoLancamento"), col("notaMedia").desc(), col("numeroVotos").desc())

# Criando as colunas 'primeira_palavra_genero' e 'segunda_palavra_genero' a partir da coluna 'genero'
movies = movies.withColumn("primeira_palavra_genero", split(col("genero"), ",").getItem(0))

# Filtrando os filmes que possuem a palavra 'War' na primeira ou segunda palavra do gênero
movies = movies.withColumn("segunda_palavra_genero", split(col("genero"), ",").getItem(1))
movies = movies.filter((col("primeira_palavra_genero").contains("War")) | (col("segunda_palavra_genero").contains("War")))

# Limitando os registros para 100000
movies = movies.limit(100000)

# Gravando o arquivo tratado no S3
movies.write.mode("overwrite").parquet(f"s3://{bucket_name}/{folder_name}/tratados/")

job.commit()