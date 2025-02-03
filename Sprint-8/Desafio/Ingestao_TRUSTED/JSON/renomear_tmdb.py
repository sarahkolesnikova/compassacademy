import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['SOURCE_BUCKET', 'SOURCE_KEY1', 'TARGET_KEY1', 
                                     'SOURCE_KEY2', 'TARGET_KEY2', 'SOURCE_KEY3', 'TARGET_KEY3'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Variáveis
bucket_name = args['SOURCE_BUCKET']  # Nome do bucket
source_key1 = args['SOURCE_KEY1']  # Caminho do arquivo original
target_key1 = args['TARGET_KEY1']  # Caminho do arquivo renomeado
source_key2 = args['SOURCE_KEY2']  # Caminho do arquivo original
target_key2 = args['TARGET_KEY2']  # Caminho do arquivo renomeado
source_key3 = args['SOURCE_KEY3']  # Caminho do arquivo original
target_key3 = args['TARGET_KEY3']  # Caminho do arquivo renomeado

# Inicializando o cliente do S3
s3 = boto3.client('s3')

# Copiando o arquivo para o novo nome
s3.copy_object(
    Bucket=bucket_name,
    CopySource={'Bucket': bucket_name, 'Key': source_key1},
    Key=target_key1
)

# Excluindo o arquivo original
s3.delete_object(
    Bucket=bucket_name,
    Key=source_key1
)
s3.copy_object(
    Bucket=bucket_name,
    CopySource={'Bucket': bucket_name, 'Key': source_key2},
    Key=target_key2
)

# Excluindo o arquivo original
s3.delete_object(
    Bucket=bucket_name,
    Key=source_key2
)
s3.copy_object(
    Bucket=bucket_name,
    CopySource={'Bucket': bucket_name, 'Key': source_key3},
    Key=target_key3
)

# Excluindo o arquivo original
s3.delete_object(
    Bucket=bucket_name,
    Key=source_key3
)

job.commit()