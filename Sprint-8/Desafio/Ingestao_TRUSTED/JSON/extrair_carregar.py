import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH1', 'S3_INPUT_PATH2', 'S3_INPUT_PATH3', 'S3_TARGET_PATH1', 'S3_TARGET_PATH2'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Entradas e saídas
source_file_1 = args['S3_INPUT_PATH1']
source_file_2 = args['S3_INPUT_PATH2']
source_file_3 = args['S3_INPUT_PATH3']
target_path1 = args['S3_TARGET_PATH1']
target_path2 = args['S3_TARGET_PATH2']

# Carregando os arquivos JSON sobre os detalhes dos filmes para o Glue DynamicFrame
df1 = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [source_file_1]},
    format="json"
)

# Convertendo para DataFrame
df1 = df1.toDF()
df1_limited = df1.limit(100000)

# Convertendo para DynamicFrame
df1_limited = DynamicFrame.fromDF(df1_limited, glueContext, "df1_limited")

# Escrevendo o arquivo no S3
glueContext.write_dynamic_frame.from_options(
        frame= df1_limited,
        connection_type='s3',
        connection_options={"path":target_path1},
        format="parquet"
        )

# Carregando os arquivos JSON sobre os stremings para o Glue DynamicFrame        
df2 = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [source_file_2]},
    format="json"
)

# Convertendo para DataFrame
df2 = df2.toDF()
df2_limited = df2.limit(100000)

# Convertendo para DynamicFrame
df2_limited = DynamicFrame.fromDF(df2_limited, glueContext, "df2_limited")

# Escrevendo o arquivo no S3
glueContext.write_dynamic_frame.from_options(
        frame= df2_limited,
        connection_type='s3',
        connection_options={"path":target_path2},
        format="parquet"
        )

# Carregando os arquivos JSON sobre as reviews para o Glue DynamicFrame        
df3 = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [source_file_3]},
    format="json"
)

# Convertendo para DataFrame
df3 = df3.toDF()
df3_limited = df3.limit(100000)

# Convertendo para DynamicFrame
df3_limited = DynamicFrame.fromDF(df3_limited, glueContext, "df3_limited")

# Escrevendo o arquivo no S3
glueContext.write_dynamic_frame.from_options(
        frame= df3_limited,
        connection_type='s3',
        connection_options={"path":target_path2},
        format="parquet"
        )

job.commit()