
# importando bibliotecas
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, round, when, regexp_replace, split, lit, collect_list, array, concat, to_date
from pyspark.sql.types import IntegerType
import pandas as pd
from functools import reduce
from pyspark.sql import DataFrame
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime


# --S3_INPUT_PATH : s3://desafio-final-sarah/Refined/temp/
# --S3_INPUT_PATH1 : s3://desafio-final-sarah/Refined/dim_filme/
# --S3_TARGET_PATH : s3://desafio-final-sarah/Refined/dim_tempo/dim_tempo/
# --S3_TARGET_PATH1 : s3://desafio-final-sarah/Refined/fato_filmes/fato_filmes/
# --S3_TARGET_PATH2 : s3://desafio-final-sarah/Refined/dim_filme


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_TARGET_PATH', 'S3_TARGET_PATH1', 'S3_TARGET_PATH2', 'S3_INPUT_PATH', 'S3_INPUT_PATH1'])


# Inicializando o SparkContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# CRIANDO DATAFRAME DA DIMENSÃO TEMPO

data = [
    {"Ano": 2000, "Mes": "01", "Nome": "Janeiro", "Trimestre": "Primeiro"},
    {"Ano": 2000, "Mes": "02", "Nome": "Fevereiro", "Trimestre": "Primeiro"},
    {"Ano": 2000, "Mes": "03", "Nome": "Março", "Trimestre": "Primeiro"},
    {"Ano": 2000, "Mes": "04", "Nome": "Abril", "Trimestre": "Segundo"},
    {"Ano": 2000, "Mes": "05", "Nome": "Maio", "Trimestre": "Segundo"},
    {"Ano": 2000, "Mes": "06", "Nome": "Junho", "Trimestre": "Segundo"},
    {"Ano": 2000, "Mes": "07", "Nome": "Julho", "Trimestre": "Terceiro"},
    {"Ano": 2000, "Mes": "08", "Nome": "Agosto", "Trimestre": "Terceiro"},
    {"Ano": 2000, "Mes": "09", "Nome": "Setembro", "Trimestre": "Terceiro"},
    {"Ano": 2000, "Mes": "10", "Nome": "Outubro", "Trimestre": "Quarto"},
    {"Ano": 2000, "Mes": "11", "Nome": "Novembro", "Trimestre": "Quarto"},
    {"Ano": 2000, "Mes": "12", "Nome": "Dezembro", "Trimestre": "Quarto"},
    {"Ano": 2001, "Mes": "01", "Nome": "Janeiro", "Trimestre": "Primeiro"},
    {"Ano": 2001, "Mes": "02", "Nome": "Fevereiro", "Trimestre": "Primeiro"},
    {"Ano": 2001, "Mes": "03", "Nome": "Março", "Trimestre": "Primeiro"},
    {"Ano": 2001, "Mes": "04", "Nome": "Abril", "Trimestre": "Segundo"},
    {"Ano": 2001, "Mes": "05", "Nome": "Maio", "Trimestre": "Segundo"},
    {"Ano": 2001, "Mes": "06", "Nome": "Junho", "Trimestre": "Segundo"},
    {"Ano": 2001, "Mes": "07", "Nome": "Julho", "Trimestre": "Terceiro"},
    {"Ano": 2001, "Mes": "08", "Nome": "Agosto", "Trimestre": "Terceiro"},
    {"Ano": 2001, "Mes": "09", "Nome": "Setembro", "Trimestre": "Terceiro"},
    {"Ano": 2001, "Mes": "10", "Nome": "Outubro", "Trimestre": "Quarto"},
    {"Ano": 2001, "Mes": "11", "Nome": "Novembro", "Trimestre": "Quarto"},
    {"Ano": 2001, "Mes": "12", "Nome": "Dezembro", "Trimestre": "Quarto"},
    {"Ano": 2002, "Mes": "01", "Nome": "Janeiro", "Trimestre": "Primeiro"},
    {"Ano": 2002, "Mes": "02", "Nome": "Fevereiro", "Trimestre": "Primeiro"},
    {"Ano": 2002, "Mes": "03", "Nome": "Março", "Trimestre": "Primeiro"},
    {"Ano": 2002, "Mes": "04", "Nome": "Abril", "Trimestre": "Segundo"},
    {"Ano": 2002, "Mes": "05", "Nome": "Maio", "Trimestre": "Segundo"},
    {"Ano": 2002, "Mes": "06", "Nome": "Junho", "Trimestre": "Segundo"},
    {"Ano": 2002, "Mes": "07", "Nome": "Julho", "Trimestre": "Terceiro"},
    {"Ano": 2002, "Mes": "08", "Nome": "Agosto", "Trimestre": "Terceiro"},
    {"Ano": 2002, "Mes": "09", "Nome": "Setembro", "Trimestre": "Terceiro"},
    {"Ano": 2002, "Mes": "10", "Nome": "Outubro", "Trimestre": "Quarto"},
    {"Ano": 2002, "Mes": "11", "Nome": "Novembro", "Trimestre": "Quarto"},
    {"Ano": 2002, "Mes": "12", "Nome": "Dezembro", "Trimestre": "Quarto"} ,
    {"Ano": 2003, "Mes": "01" , "Nome": "Janeiro", "Trimestre": "Primeiro"} ,
    {"Ano": 2003, "Mes": "02" , "Nome": "Fevereiro", "Trimestre": "Primeiro"},
    {"Ano": 2003, "Mes": "03", "Nome": "Março", "Trimestre": "Primeiro"},
    {"Ano": 2003, "Mes": "04", "Nome": "Abril", "Trimestre": "Segundo"},
    {"Ano": 2003, "Mes": "05", "Nome": "Maio", "Trimestre": "Segundo"},
    {"Ano": 2003, "Mes": "06", "Nome": "Junho", "Trimestre": "Segundo"},
    {"Ano": 2003, "Mes": "07", "Nome": "Julho", "Trimestre": "Terceiro"},
    {"Ano": 2003, "Mes": "08", "Nome": "Agosto", "Trimestre": "Terceiro"},
    {"Ano": 2003, "Mes": "09", "Nome": "Setembro", "Trimestre": "Terceiro"},
    {"Ano": 2003, "Mes": "10", "Nome": "Outubro", "Trimestre": "Quarto"},
    {"Ano": 2003, "Mes": "11", "Nome": "Novembro", "Trimestre": "Quarto"},
    {"Ano": 2003, "Mes": "12", "Nome": "Dezembro", "Trimestre": "Quarto"},
    {"Ano": 2004, "Mes": "01", "Nome": "Janeiro", "Trimestre": "Primeiro"},
    {"Ano": 2004, "Mes": "02", "Nome": "Fevereiro", "Trimestre": "Primeiro"},
    {"Ano": 2004, "Mes": "03", "Nome": "Março", "Trimestre": "Primeiro"},
    {"Ano": 2004, "Mes": "04", "Nome": "Abril", "Trimestre": "Segundo"},
    {"Ano": 2004, "Mes": "05", "Nome": "Maio", "Trimestre": "Segundo"},
    {"Ano": 2004, "Mes": "06", "Nome": "Junho", "Trimestre": "Segundo"},
    {"Ano": 2004, "Mes": "07", "Nome": "Julho", "Trimestre": "Terceiro"},
    {"Ano": 2004, "Mes": "08", "Nome": "Agosto", "Trimestre": "Terceiro"},
    {"Ano": 2004, "Mes": "09", "Nome": "Setembro", "Trimestre": "Terceiro"},
    {"Ano": 2004, "Mes": "10", "Nome": "Outubro", "Trimestre": "Quarto"},
    {"Ano": 2004, "Mes": "11", "Nome": "Novembro", "Trimestre": "Quarto"},
    {"Ano": 2004, "Mes": "12", "Nome": "Dezembro", "Trimestre": "Quarto"},
    {"Ano": 2005, "Mes": "01", "Nome": "Janeiro", "Trimestre": "Primeiro"},
    {"Ano": 2005, "Mes": "02", "Nome": "Fevereiro", "Trimestre": "Primeiro"},
    {"Ano": 2005, "Mes": "03", "Nome": "Março", "Trimestre": "Primeiro"},
    {"Ano": 2005, "Mes": '04', "Nome": "Abril", "Trimestre": "Segundo"},
    {"Ano": 2005, "Mes": "05", "Nome": "Maio", "Trimestre": "Segundo"},
    {"Ano": 2005, "Mes": "06", "Nome": "Junho", "Trimestre": "Segundo"},
    {"Ano": 2005, "Mes": "07", "Nome": "Julho", "Trimestre": "Terceiro"},
    {"Ano": 2005, "Mes": "08", "Nome": "Agosto", "Trimestre": "Terceiro"},
    {"Ano": 2005, "Mes": "09", "Nome": "Setembro", "Trimestre": "Terceiro"},
    {"Ano": 2005, "Mes": "10", "Nome": "Outubro", "Trimestre": "Quarto"},
    {"Ano": 2005, "Mes": "11", "Nome": "Novembro", "Trimestre": "Quarto"},
    {"Ano": 2005, "Mes": "12", "Nome": "Dezembro", "Trimestre": "Quarto"},
    {"Ano": 2006, "Mes": "01", "Nome": "Janeiro", "Trimestre": "Primeiro"},
    {"Ano": 2006, "Mes": "02", "Nome": "Fevereiro", "Trimestre": "Primeiro"},
    {"Ano": 2006, "Mes": "03", "Nome": "Março", "Trimestre": "Primeiro"},
    {"Ano": 2006, "Mes": "04", "Nome": "Abril", "Trimestre": "Segundo"},
    {"Ano": 2006, "Mes": "05", "Nome": "Maio", "Trimestre": "Segundo"},
    {"Ano": 2006, "Mes": "06", "Nome": "Junho", "Trimestre": "Segundo"},
    {"Ano": 2006, "Mes": "07", "Nome": "Julho", "Trimestre": "Terceiro"},
    {"Ano": 2006, "Mes": "08", "Nome": "Agosto", "Trimestre": "Terceiro"},
    {"Ano": 2006, "Mes": "09", "Nome": "Setembro", "Trimestre": "Terceiro"},
    {"Ano": 2006, "Mes": "10", "Nome": "Outubro", "Trimestre": "Quarto"},
    {"Ano": 2006, "Mes": "11", "Nome": "Novembro", "Trimestre": "Quarto"},
    {"Ano": 2006, "Mes": "12", "Nome": "Dezembro", "Trimestre": "Quarto"},
    {"Ano": 2007, "Mes": "01", "Nome": "Janeiro", "Trimestre": "Primeiro"},
    {"Ano": 2007, "Mes": "02", "Nome": "Fevereiro", "Trimestre": "Primeiro"},
    {"Ano": 2007, "Mes": "03", "Nome": "Março", "Trimestre": "Primeiro"},
    {"Ano": 2007, "Mes": "04", "Nome": "Abril", "Trimestre": "Segundo"},
    {"Ano": 2007, "Mes": "05", "Nome": "Maio", "Trimestre": "Segundo"},
    {"Ano": 2007, "Mes": "06", "Nome": "Junho", "Trimestre": "Segundo"},
    {"Ano": 2007, "Mes": "07", "Nome": "Julho", "Trimestre": "Terceiro"},
    {"Ano": 2007, "Mes": "08", "Nome": "Agosto", "Trimestre": "Terceiro"},
    {"Ano": 2007, "Mes": "09", "Nome": "Setembro", "Trimestre": "Terceiro"},
    {"Ano": 2007, "Mes": "10", "Nome": "Outubro", "Trimestre": "Quarto"},
    {"Ano": 2007, "Mes": "11", "Nome": "Novembro", "Trimestre": "Quarto"},
    {"Ano": 2007, "Mes": "12", "Nome": "Dezembro", "Trimestre": "Quarto"},
    {"Ano": 2008, "Mes": "01", "Nome": "Janeiro", "Trimestre": "Primeiro"},
    {"Ano": 2008, "Mes": "02", "Nome": "Fevereiro", "Trimestre": "Primeiro"},
    {"Ano": 2008, "Mes": "03", "Nome": "Março", "Trimestre": "Primeiro"},
    {"Ano": 2008, "Mes": "04", "Nome": "Abril", "Trimestre": "Segundo"},
    {"Ano": 2008, "Mes": "05", "Nome": "Maio", "Trimestre": "Segundo"},
    {"Ano": 2008, "Mes": "06", "Nome": "Junho", "Trimestre": "Segundo"},
    {"Ano": 2008, "Mes": "07", "Nome": "Julho", "Trimestre": "Terceiro"},
    {"Ano": 2008, "Mes": "08", "Nome": "Agosto", "Trimestre": "Terceiro"},
    {"Ano": 2008, "Mes": "09", "Nome": "Setembro", "Trimestre": "Terceiro"},
    {"Ano": 2008, "Mes": "10", "Nome": "Outubro", "Trimestre": "Quarto"},
    {"Ano": 2008, "Mes": "11", "Nome": "Novembro", "Trimestre": "Quarto"},
    {"Ano": 2008, "Mes": "12", "Nome": "Dezembro", "Trimestre": "Quarto"},
    {"Ano": 2009, "Mes": "01", "Nome": "Janeiro", "Trimestre": "Primeiro"},
    {"Ano": 2009, "Mes": "02", "Nome": "Fevereiro", "Trimestre": "Primeiro"},
    {"Ano": 2009, "Mes": "03", "Nome": "Março", "Trimestre": "Primeiro"},
    {"Ano": 2009, "Mes": "04", "Nome": "Abril", "Trimestre": "Segundo"},
    {"Ano": 2009, "Mes": "05", "Nome": "Maio", "Trimestre": "Segundo"},
    {"Ano": 2009, "Mes": "06", "Nome": "Junho", "Trimestre": "Segundo"},
    {"Ano": 2009, "Mes": "07", "Nome": "Julho", "Trimestre": "Terceiro"},
    {"Ano": 2009, "Mes": "08", "Nome": "Agosto", "Trimestre": "Terceiro"},
    {"Ano": 2009, "Mes": "09", "Nome": "Setembro", "Trimestre": "Terceiro"},
    {"Ano": 2009, "Mes": "10", "Nome": "Outubro", "Trimestre": "Quarto"},
    {"Ano": 2009, "Mes": "11", "Nome": "Novembro", "Trimestre": "Quarto"},
    {"Ano": 2009, "Mes": "12", "Nome": "Dezembro", "Trimestre": "Quarto"},
    {"Ano": 2010, "Mes": "01", "Nome": "Janeiro", "Trimestre": "Primeiro"},
    {"Ano": 2010, "Mes": "02", "Nome": "Fevereiro", "Trimestre": "Primeiro"},
    {"Ano": 2010, "Mes": "03", "Nome": "Março", "Trimestre": "Primeiro"},
    {"Ano": 2010, "Mes": "04", "Nome": "Abril", "Trimestre": "Segundo"},
    {"Ano": 2010, "Mes": "05", "Nome": "Maio", "Trimestre": "Segundo"},
    {"Ano": 2010, "Mes": "06", "Nome": "Junho", "Trimestre": "Segundo"},
    {"Ano": 2010, "Mes": "07", "Nome": "Julho", "Trimestre": "Terceiro"},
    {"Ano": 2010, "Mes": "08", "Nome": "Agosto", "Trimestre": "Terceiro"},
    {"Ano": 2010, "Mes": "09", "Nome": "Setembro", "Trimestre": "Terceiro"},
    {"Ano": 2010, "Mes": "10", "Nome": "Outubro", "Trimestre": "Quarto"},
    {"Ano": 2010, "Mes": "11", "Nome": "Novembro", "Trimestre": "Quarto"},
    {"Ano": 2010, "Mes": "12", "Nome": "Dezembro", "Trimestre": "Quarto"},
    {"Ano": 2011, "Mes": "01", "Nome": "Janeiro", "Trimestre": "Primeiro"},
    {"Ano": 2011, "Mes": "02", "Nome": "Fevereiro", "Trimestre": "Primeiro"},
    {"Ano": 2011, "Mes": "03", "Nome": "Março", "Trimestre": "Primeiro"},
    {"Ano": 2011, "Mes": "04", "Nome": "Abril", "Trimestre": "Segundo"},
    {"Ano": 2011, "Mes": "05", "Nome": "Maio", "Trimestre": "Segundo"},
    {"Ano": 2011, "Mes": "06", "Nome": "Junho", "Trimestre": "Segundo"},
    {"Ano": 2011, "Mes": "07", "Nome": "Julho", "Trimestre": "Terceiro"},
    {"Ano": 2011, "Mes": "08", "Nome": "Agosto", "Trimestre": "Terceiro"},
    {"Ano": 2011, "Mes": "09", "Nome": "Setembro", "Trimestre": "Terceiro"},
    {"Ano": 2011, "Mes": "10", "Nome": "Outubro", "Trimestre": "Quarto"},
    {"Ano": 2011, "Mes": "11", "Nome": "Novembro", "Trimestre": "Quarto"},
    {"Ano": 2011, "Mes": "12", "Nome": "Dezembro", "Trimestre": "Quarto"},
    {"Ano": 2012, "Mes": "01", "Nome": "Janeiro", "Trimestre": "Primeiro"},
    {"Ano": 2012, "Mes": "02", "Nome": "Fevereiro", "Trimestre": "Primeiro"},
    {"Ano": 2012, "Mes": "03", "Nome": "Março", "Trimestre": "Primeiro"},
    {"Ano": 2012, "Mes": "04", "Nome": "Abril", "Trimestre": "Segundo"},
    {"Ano": 2012, "Mes": "05", "Nome": "Maio", "Trimestre": "Segundo"},
    {"Ano": 2012, "Mes": "06", "Nome": "Junho", "Trimestre": "Segundo"},
    {"Ano": 2012, "Mes": "07", "Nome": "Julho", "Trimestre": "Terceiro"},
    {"Ano": 2012, "Mes": "08", "Nome": "Agosto", "Trimestre": "Terceiro"},
    {"Ano": 2012, "Mes": "09", "Nome": "Setembro", "Trimestre": "Terceiro"},
    {"Ano": 2012, "Mes": "10", "Nome": "Outubro", "Trimestre": "Quarto"},
    {"Ano": 2012, "Mes": "11", "Nome": "Novembro", "Trimestre": "Quarto"},
    {"Ano": 2012, "Mes": "12", "Nome": "Dezembro", "Trimestre": "Quarto"},
    {"Ano": 2013, "Mes": "01", "Nome": "Janeiro", "Trimestre": "Primeiro"},
    {"Ano": 2013, "Mes": "02", "Nome": "Fevereiro", "Trimestre": "Primeiro"},
    {"Ano": 2013, "Mes": "03", "Nome": "Março", "Trimestre": "Primeiro"},
    {"Ano": 2013, "Mes": "04", "Nome": "Abril", "Trimestre": "Segundo"},
    {"Ano": 2013, "Mes": "05", "Nome": "Maio", "Trimestre": "Segundo"},
    {"Ano": 2013, "Mes": "06", "Nome": "Junho", "Trimestre": "Segundo"},
    {"Ano": 2013, "Mes": "07", "Nome": "Julho", "Trimestre": "Terceiro"},
    {"Ano": 2013, "Mes": "08", "Nome": "Agosto", "Trimestre": "Terceiro"},
    {"Ano": 2013, "Mes": "09", "Nome": "Setembro", "Trimestre": "Terceiro"},
    {"Ano": 2013, "Mes": "10", "Nome": "Outubro", "Trimestre": "Quarto"},
    {"Ano": 2013, "Mes": "11", "Nome": "Novembro", "Trimestre": "Quarto"},
    {"Ano": 2013, "Mes": "12", "Nome": "Dezembro", "Trimestre": "Quarto"},
    {"Ano": 2014, "Mes": "01", "Nome": "Janeiro", "Trimestre": "Primeiro"},
    {"Ano": 2014, "Mes": "02", "Nome": "Fevereiro", "Trimestre": "Primeiro"},
    {"Ano": 2014, "Mes": "03", "Nome": "Março", "Trimestre": "Primeiro"},
    {"Ano": 2014, "Mes": "04", "Nome": "Abril", "Trimestre": "Segundo"},
    {"Ano": 2014, "Mes": "05", "Nome": "Maio", "Trimestre": "Segundo"},
    {"Ano": 2014, "Mes": "06", "Nome": "Junho", "Trimestre": "Segundo"},
    {"Ano": 2014, "Mes": "07", "Nome": "Julho", "Trimestre": "Terceiro"},
    {"Ano": 2014, "Mes": "08", "Nome": "Agosto", "Trimestre": "Terceiro"},
    {"Ano": 2014, "Mes": "09", "Nome": "Setembro", "Trimestre": "Terceiro"},
    {"Ano": 2014, "Mes": "10", "Nome": "Outubro", "Trimestre": "Quarto"},
    {"Ano": 2014, "Mes": "11", "Nome": "Novembro", "Trimestre": "Quarto"},
    {"Ano": 2014, "Mes": "12", "Nome": "Dezembro", "Trimestre": "Quarto"},
    {"Ano": 2015, "Mes": "01", "Nome": "Janeiro", "Trimestre": "Primeiro"},
    {"Ano": 2015, "Mes": "02", "Nome": "Fevereiro", "Trimestre": "Primeiro"},
    {"Ano": 2015, "Mes": "03", "Nome": "Março", "Trimestre": "Primeiro"},
    {"Ano": 2015, "Mes": "04", "Nome": "Abril", "Trimestre": "Segundo"},
    {"Ano": 2015, "Mes": "05", "Nome": "Maio", "Trimestre": "Segundo"},
    {"Ano": 2015, "Mes": "06", "Nome": "Junho", "Trimestre": "Segundo"},
    {"Ano": 2015, "Mes": "07", "Nome": "Julho", "Trimestre": "Terceiro"},
    {"Ano": 2015, "Mes": "08", "Nome": "Agosto", "Trimestre": "Terceiro"},
    {"Ano": 2015, "Mes": "09", "Nome": "Setembro", "Trimestre": "Terceiro"},
    {"Ano": 2015, "Mes": "10", "Nome": "Outubro", "Trimestre": "Quarto"},
    {"Ano": 2015, "Mes": "11", "Nome": "Novembro", "Trimestre": "Quarto"},
    {"Ano": 2015, "Mes": "12", "Nome": "Dezembro", "Trimestre": "Quarto"},
    {"Ano": 2016, "Mes": "01", "Nome": "Janeiro", "Trimestre": "Primeiro"},
    {"Ano": 2016, "Mes": "02", "Nome": "Fevereiro", "Trimestre": "Primeiro"},
    {"Ano": 2016, "Mes": "03", "Nome": "Março", "Trimestre": "Primeiro"},
    {"Ano": 2016, "Mes": "04", "Nome": "Abril", "Trimestre": "Segundo"},
    {"Ano": 2016, "Mes": "05", "Nome": "Maio", "Trimestre": "Segundo"},
    {"Ano": 2016, "Mes": "06", "Nome": "Junho", "Trimestre": "Segundo"},
    {"Ano": 2016, "Mes": "07", "Nome": "Julho", "Trimestre": "Terceiro"},
    {"Ano": 2016, "Mes": "08", "Nome": "Agosto", "Trimestre": "Terceiro"},
    {"Ano": 2016, "Mes": "09", "Nome": "Setembro", "Trimestre": "Terceiro"},
    {"Ano": 2016, "Mes": "10", "Nome": "Outubro", "Trimestre": "Quarto"},
    {"Ano": 2016, "Mes": "11", "Nome": "Novembro", "Trimestre": "Quarto"},
    {"Ano": 2016, "Mes": "12", "Nome": "Dezembro", "Trimestre": "Quarto"},
    {"Ano": 2017, "Mes": "01", "Nome": "Janeiro", "Trimestre": "Primeiro"},
    {"Ano": 2017, "Mes": "02", "Nome": "Fevereiro", "Trimestre": "Primeiro"},
    {"Ano": 2017, "Mes": "03", "Nome": "Março", "Trimestre": "Primeiro"},
    {"Ano": 2017, "Mes": "04", "Nome": "Abril", "Trimestre": "Segundo"},
    {"Ano": 2017, "Mes": "05", "Nome": "Maio", "Trimestre": "Segundo"},
    {"Ano": 2017, "Mes": "06", "Nome": "Junho", "Trimestre": "Segundo"},
    {"Ano": 2017, "Mes": "07", "Nome": "Julho", "Trimestre": "Terceiro"},
    {"Ano": 2017, "Mes": "08", "Nome": "Agosto", "Trimestre": "Terceiro"},
    {"Ano": 2017, "Mes": "09", "Nome": "Setembro", "Trimestre": "Terceiro"},
    {"Ano": 2017, "Mes": "10", "Nome": "Outubro", "Trimestre": "Quarto"},
    {"Ano": 2017, "Mes": "11", "Nome": "Novembro", "Trimestre": "Quarto"},
    {"Ano": 2017, "Mes": "12", "Nome": "Dezembro", "Trimestre": "Quarto"},
    {"Ano": 2018, "Mes": "01", "Nome": "Janeiro", "Trimestre": "Primeiro"},
    {"Ano": 2018, "Mes": "02", "Nome": "Fevereiro", "Trimestre": "Primeiro"},
    {"Ano": 2018, "Mes": "03", "Nome": "Março", "Trimestre": "Primeiro"},
    {"Ano": 2018, "Mes": "04", "Nome": "Abril", "Trimestre": "Segundo"},
    {"Ano": 2018, "Mes": "05", "Nome": "Maio", "Trimestre": "Segundo"},
    {"Ano": 2018, "Mes": "06", "Nome": "Junho", "Trimestre": "Segundo"},
    {"Ano": 2018, "Mes": "07", "Nome": "Julho", "Trimestre": "Terceiro"},
    {"Ano": 2018, "Mes": "08", "Nome": "Agosto", "Trimestre": "Terceiro"},
    {"Ano": 2018, "Mes": "09", "Nome": "Setembro", "Trimestre": "Terceiro"},
    {"Ano": 2018, "Mes": "10", "Nome": "Outubro", "Trimestre": "Quarto"},
    {"Ano": 2018, "Mes": "11", "Nome": "Novembro", "Trimestre": "Quarto"},
    {"Ano": 2018, "Mes": "12", "Nome": "Dezembro", "Trimestre": "Quarto"},
    {"Ano": 2019, "Mes": "01", "Nome": "Janeiro", "Trimestre": "Primeiro"},
    {"Ano": 2019, "Mes": "02", "Nome": "Fevereiro", "Trimestre": "Primeiro"},
    {"Ano": 2019, "Mes": "03", "Nome": "Março", "Trimestre": "Primeiro"},
    {"Ano": 2019, "Mes": "04", "Nome": "Abril", "Trimestre": "Segundo"},
    {"Ano": 2019, "Mes": "05", "Nome": "Maio", "Trimestre": "Segundo"},
    {"Ano": 2019, "Mes": "06", "Nome": "Junho", "Trimestre": "Segundo"},
    {"Ano": 2019, "Mes": "07", "Nome": "Julho", "Trimestre": "Terceiro"},
    {"Ano": 2019, "Mes": "08", "Nome": "Agosto", "Trimestre": "Terceiro"},
    {"Ano": 2019, "Mes": "09", "Nome": "Setembro", "Trimestre": "Terceiro"},
    {"Ano": 2019, "Mes": "10", "Nome": "Outubro", "Trimestre": "Quarto"},
    {"Ano": 2019, "Mes": "11", "Nome": "Novembro", "Trimestre": "Quarto"},
    {"Ano": 2019, "Mes": "12", "Nome": "Dezembro", "Trimestre": "Quarto"},
    {"Ano": 2020, "Mes": "01", "Nome": "Janeiro", "Trimestre": "Primeiro"},
    {"Ano": 2020, "Mes": "02", "Nome": "Fevereiro", "Trimestre": "Primeiro"},
    {"Ano": 2020, "Mes": "03", "Nome": "Março", "Trimestre": "Primeiro"},
    {"Ano": 2020, "Mes": "04", "Nome": "Abril", "Trimestre": "Segundo"},
    {"Ano": 2020, "Mes": "05", "Nome": "Maio", "Trimestre": "Segundo"},
    {"Ano": 2020, "Mes": "06", "Nome": "Junho", "Trimestre": "Segundo"},
    {"Ano": 2020, "Mes": "07", "Nome": "Julho", "Trimestre": "Terceiro"},
    {"Ano": 2020, "Mes": "08", "Nome": "Agosto", "Trimestre": "Terceiro"},
    {"Ano": 2020, "Mes": "09", "Nome": "Setembro", "Trimestre": "Terceiro"},
    {"Ano": 2020, "Mes": "10", "Nome": "Outubro", "Trimestre": "Quarto"},
    {"Ano": 2020, "Mes": "11", "Nome": "Novembro", "Trimestre": "Quarto"},
    {"Ano": 2020, "Mes": "12", "Nome": "Dezembro", "Trimestre": "Quarto"}
    ]
df = pd.DataFrame(data)
id_lista_tempo = list(range(1,253))
df_tempo = pd.DataFrame({'id_tempo': id_lista_tempo[:len(df)], 
                         'ano': df['Ano'].tolist(), 
                         'mes': df['Mes'].tolist(),
                         'nome': df['Nome'].tolist(),
                         'trimestre': df['Trimestre'].tolist()})


# SALVANDO OS DADOS NO S3

df_tempo = spark.createDataFrame(df_tempo)
output = args['S3_TARGET_PATH']
df1 = df_tempo
df_limited = DynamicFrame.fromDF(df1.limit(100000), glueContext, "df_limited") # limitando o número de linhas

glueContext.write_dynamic_frame.from_options( 
        frame= df_limited,
        connection_type='s3',
        connection_options={"path": output},
        format="parquet"
        )
df_tempo1 = df_tempo.toPandas()       
df_tempo1['ano_mes'] = df_tempo1['ano'].astype(str) + '-' + df_tempo1['mes'].astype(str)

df_tempo1 = spark.createDataFrame(df_tempo1)


# TRATANDO OS DADOS DA TABELA DE FATO
dados = args['S3_INPUT_PATH']
dados1 = spark.read.parquet(dados)
fato_filmes = dados1.select("id", "ano", "mes", "vote_average", "budget", "revenue", "popularity", "vote_count", "id_guerra")


fato_filmes = fato_filmes.withColumn("dataLancamento", concat(col("ano"), lit("-"), col("mes")))
fato_filmes = fato_filmes.withColumn("dataLancamento", to_date(col("dataLancamento"), "yyyy-MM"))

apoio = fato_filmes.join(df_tempo1, fato_filmes.dataLancamento == df_tempo1.ano_mes, "inner")
fato_filmes1 = apoio.select("id", "vote_average", "budget", "revenue", "popularity", "vote_count", "id_guerra", "id_tempo")
fato_filmes1 = fato_filmes1.withColumnRenamed("id", "id_tmdb")
fato_filmes1 = fato_filmes1.withColumnRenamed("vote_average", "media_votos")
fato_filmes1 = fato_filmes1.withColumnRenamed("budget", "orcamento")
fato_filmes1 = fato_filmes1.withColumnRenamed("revenue", "bilheteria")
fato_filmes1 = fato_filmes1.withColumnRenamed("popularity", "popularidade")
fato_filmes1 = fato_filmes1.withColumnRenamed("vote_count", "votos")

fato_filmes1 = fato_filmes1.withColumn("id_guerra", when(col("id_guerra") == 5, 2).otherwise(col("id_guerra")))
fato_filmes1 = fato_filmes1.withColumn(
    "id_guerra",
    when(
        col("id_tmdb").isin([8342, 215814, 449498, 378636, 431399, 627816]),
        1
    ).otherwise(col("id_guerra"))
)
fato_filmes1 = fato_filmes1.withColumn(
    "id_guerra", when(
        col("id_tmdb").isin([478067]), 5
    ).otherwise(col("id_guerra"))
)

# SALVANDO OS DADOS NO S3

output1 = args['S3_TARGET_PATH1']
df2 = fato_filmes1
df_limited1 = DynamicFrame.fromDF(df2.limit(100000), glueContext, "df_limited1")  # limitando o número de linhas

glueContext.write_dynamic_frame.from_options(
    frame=df_limited1,
    connection_type='s3',
    connection_options={"path": output1},
    format="parquet"
)

# TRATANDO OS DADOS DA TABELA DIMENSÃO FILME

filmes= args['S3_INPUT_PATH1']
filmes1 = spark.read.parquet(filmes)
filmes1 = filmes1.withColumnRenamed("title","titulo")
filmes1 = filmes1.withColumnRenamed("original_language","idioma")
filmes1 = filmes1.withColumnRenamed("original_title","titulo_original")
filmes1 = filmes1.withColumnRenamed("runtime","duracao")
filmes1 = filmes1.withColumnRenamed("overviews","sinopse")
filmes1 = filmes1.withColumnRenamed("reviews","comentarios")
filmes1 = filmes1.withColumn("comentarios", split(col("comentarios"), ":").getItem(1))


# SALVANDO OS DADOS NO S3

output2 = args['S3_TARGET_PATH2']
df3 = filmes1
df_limited2 = DynamicFrame.fromDF(df3.limit(100000), glueContext, "df_limited2") # limitando o número de linhas

glueContext.write_dynamic_frame.from_options( 
        frame= df_limited2,
        connection_type='s3',
        connection_options={"path": output2},
        format="parquet"
        )

job.commit()

