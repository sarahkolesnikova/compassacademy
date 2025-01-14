from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower

# Inicializar a sessão Spark
spark = SparkSession.builder.appName("ContarPalavras").getOrCreate()

# Ler o arquivo README.md
dados = spark.read.text("/content/drive/MyDrive/Colab Notebooks/README.md")

# Dividir as linhas em palavras
palavras = dados.select(explode(split(lower(dados["value"]), "\s+")).alias("palavras"))

# Filtrar palavras vazias
palavras_filtradas = palavras.filter(palavras["palavras"] != "")

# Contar as palavras
palavrasContadas = palavras_filtradas.groupBy("palavras").count()

# Ordenar por contagem
palavrasContadasOrg = palavrasContadas.orderBy("count", ascending=False)

# Mostrar os resultados
palavrasContadasOrg.show(palavrasContadasOrg.count())

# Parar a sessão Spark
spark.stop()