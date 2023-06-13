# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## 1 - Leitura das Bases

# COMMAND ----------

# Visualizando a Tabela Orders da Base de Dados Bronze

df_orders = spark.sql("SELECT * FROM bronze.orders")

df_orders.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2 - Conexão Bases Externas 

# COMMAND ----------

base = "server-estoque"
database_host = "server-estoque.database.windows.net"
database_port = "1433"
database_name = "estoque"
table = "dbo.posicao_estoque"
user = "root-estoque"
password = "Amb@!Stock"

# COMMAND ----------

url = f"jdbc:sqlserver://{database_host}:{database_port};database={database_name};user={user}@{base};password={password};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

url

# COMMAND ----------

driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

# COMMAND ----------

# SQL SERVER

posicao_estoque = (spark.read\
                   .format('jdbc')\
                   .option('driver', driver)\
                   .option('url', url)\
                   .option('dbtable', table)\
                   .option('user', user)\
                   .option('password', password)\
                   .load())

posicao_estoque.display()

# COMMAND ----------

# ORACLE

# https://www.cdata.com/kb/tech/oracledb-jdbc-azure-databricks.rst (OCI)

df_oracle = (spark.read\
    .format("jdbc")\
    .option("url", "jdbc:oracle:thin:username/password@//hostname:portnumber/SID")\
    .option("dbtable", "hr.emp")\
    .option("user", "db_user_name")\
    .option("password", "password")\
    .option("driver", "oracle.jdbc.driver.OracleDriver")\
    .load())

df_oracle.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 3 - Visualizando colunas (SELECT)

# COMMAND ----------

df_orders = spark.sql("SELECT * FROM bronze.orders")
df_orders.display()

# COMMAND ----------

spark.sql("SELECT * FROM bronze.orders").display()

# COMMAND ----------

df_orders.select(['order_id', 'store_id']).display()

# COMMAND ----------

# df_orders.select(['order_id', 'store_id']) (Neste modo mostra somente o tipo da coluna)

df_orders.select(['order_id', 'store_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 4 - Filtrando Valores (WHERE - Filter())

# COMMAND ----------

# Importando funções sql para o Pyspark
# Padrão functions as F or f

from pyspark.sql import functions as F


# COMMAND ----------

# No Pyspark temos a função col() que ajuda a selecionar colunas
# Com a função functions.col não precisa passar o dataframe toda hora para fazer seleção 

df_orders.filter(F.col('order_amount') > 100).display()

# COMMAND ----------

df_orders.filter((F.col('order_amount') > 100) &
                 (F.col('channel_id') == 1)).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 5 - Fazendo junções (JOIN)

# COMMAND ----------

# Verificando a tabela bronze.stores

df_full_stores = spark.sql("SELECT * FROM bronze.stores")

df_full_stores.display()


# COMMAND ----------

df_stores = spark.sql("SELECT store_id, store_name FROM bronze.stores")

df_stores.display()

# COMMAND ----------

# Fazendo join de todas as colunas
# how = pra indicar o tipo de Join
# on = pra indicar qual a coluna que faz o relacionamento entre as tabelas

df_orders.join(df_stores, on=['store_id'], how='left').display()


# COMMAND ----------

# df_orders_join = df_orders[['store_id', 'order_id', 'order_amount']].join(df_stores[['store_id']], on=['store_id'], how='left') (Chamando somente a coluna store_id do dataframe df_stores)

df_orders_join = df_orders[['store_id', 'order_id', 'order_amount']].join(df_stores, on=['store_id'], how='left')

df_orders_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 6 - Calculos Básicos e Agregações

# COMMAND ----------

df_orders = spark.sql("SELECT * FROM bronze.orders")
df_orders.display()

# COMMAND ----------

# withColumn(): https://sparkbyexamples.com/pyspark/pyspark-withcolumn/

'''
OBS: Tratamento de colunas usa o withColumn()

PySpark withColumn()é uma função de transformação do DataFrame que é usada para alterar o valor, 
converter o tipo de dados de uma coluna existente, criar uma nova coluna e muito mais

'''

# order_delivery_fee = comissão,gratificação pela entrega
# order_delivery_cost = custo direto pela entrega
# total_delivery_cost = nova coluna formada da soma de (order_delivery_fee + order_delivery_cost)

df_orders.withColumn('total_delivery_cost', F.col('order_delivery_fee') + F.col('order_delivery_cost')).display()


# COMMAND ----------

df_orders[['order_id', 'store_id', 'order_delivery_fee', 'order_delivery_cost']]\
    .withColumn('total_delivery_cost', F.col('order_delivery_fee') + F.col('order_delivery_cost'))\
    .display()

# COMMAND ----------

# Agregações exemplo 1 (withColumnRenamed) --> https://sparkbyexamples.com/pyspark/pyspark-groupby-agg-aggregate-explained/

# df_orders.groupBy(['store_id']) or df_orders.groupBy('store_id')

# {'chave':'valor'} = {'coluna':'função_agregação'}

df_orders.groupBy(['store_id'])\
    .agg({'order_amount':'sum'})\
    .withColumnRenamed('sum(order_amount)', 'total').display()  # Se não renomear o nome da coluna fica sum(order_amount)

# COMMAND ----------

# Agregações exemplo 2 (F.function_aggregation().alias())  --> from pyspark.sql import functions as F

df_orders.groupBy('store_id')\
    .agg(F.sum('order_amount').alias('total')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 7 - Tratamento de Dados Duplicados

# COMMAND ----------

df_orders = spark.sql("SELECT * FROM bronze.orders")
df_orders.display()

# COMMAND ----------

# Tem de passar o dataframe novamente para que ele agora receba a deleção dos dados duplicados

# Coalesce faz a junção das partições em somente 1 para otmização de deletar os dados duplicados

# https://www.geeksforgeeks.org/python-pandas-dataframe-drop_duplicates/

# OBS PANDAS: Temos a opção de selecionar as colunas que queremos deletar dados duplicados com 'subset' --> df.drop_duplicates(subset="name_column")

# Pyspark: dropDuplicates(['order_id','store_id'])

df_orders = df_orders.coalesce(1).dropDuplicates()

# COMMAND ----------

'''

EXEMPLO SQL + PYSPARK:


1: Criando um novo dataframe atavés de um Select no Spark/Pyspark

orders2 = spark.sql("SELECT * FROM bronze.orders LIMIT 100")
display(orders2)


2: Duplicando os dados com o novo datraframe orders2 criado, aumentando em 100 linhas duplicadas no dataframe original 

orders2.write.format("delta").mode("append").saveAsTable("bronze.orders")



3: Verificar quais são as order_id duplicadas


SELECT order_id, store_id, order_amount, COUNT(*) as qtd_registros_duplicados FROM bronze.orders GROUP BY order_id, store_id, order_amount;



4: Verificar a quantidade de linhas duplicadas de order_id (SUBQUERIES - SUBSELECT)

SELECT COUNT(*) AS Total_Duplicados
FROM(SELECT order_id, store_id, order_amount, COUNT(*) as qtd_registros_duplicados FROM bronze.orders GROUP BY order_id, store_id, order_amount) a
WHERE a.qtd_registros_duplicados > 1;




5: Criando um dataframe através do 'SELECT DISTINCT(*)' para trazer informações sem  os dados duplicados


df_orders = spark.sql(SELECT DISTINCT(*) FROM bronze.orders) 




'''

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 8 - Casting

# COMMAND ----------

# Importando as bibliotcas

# pyspark.sql.types (Importar todos os tipos de dados do pyspark-sql)

# Funções Replace Pyspark: https://sparkbyexamples.com/pyspark/pyspark-replace-column-values/

from pyspark.sql import functions as F
from pyspark.sql.types import *  

# COMMAND ----------

df_orders = spark.sql("SELECT * FROM bronze.orders")
df_orders.display()

# COMMAND ----------

df_orders[['order_amount', 'order_moment_created']] \
    .withColumn('order_amount', F.col('order_amount').cast(DoubleType())) \
    .withColumn('order_moment_created', F.to_date(F.regexp_replace(F.substring(F.col('order_moment_created'), 1, 9), ' ', ''), 'M/d/yyyy')) \
.display()

# COMMAND ----------

'''

USANDO SQL:

Exemplo 0:

SELECT
  CAST(order_amount AS FLOAT) AS order_amount,
  TO_DATE(REPLACE(SUBSTRING(order_moment_created, 1, 9), " ", ""), "M/d/yyyy") AS order_moment_created
FROM bronze.orders;


Exemplo 1:

SELECT
  CAST(order_amount AS FLOAT) AS order_amount,
  order_moment_created,
  TO_DATE(REPLACE(SUBSTRING(order_moment_created, 1, 9), " ", ""), "M/d/yyyy") AS order_moment_created_formated
FROM bronze.orders;


Exemplo 2:

SELECT
  CAST(order_amount AS FLOAT) AS order_amount,
  order_moment_created,
  TO_DATE(REPLACE(SUBSTRING(order_moment_created, 1, 9), " ", ""), "M/d/yyyy") AS order_moment_created_formated
FROM bronze.orders
WHERE
  order_created_day > 10
AND
  order_created_month > 2;



'''

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 9 - Detectando Anomalias

# COMMAND ----------

# 1 - Importar bibliotecas

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# 2 - Ver o dataframe completo

df_orders = spark.sql("SELECT * FROM bronze.orders")
df_orders.display()

# COMMAND ----------

# 3 - Fazer o casting na coluna desejada

df_orders = df_orders.withColumn('order_amount', F.col('order_amount').cast(DoubleType()))

# COMMAND ----------

# 4 - Verificando o tipos de dados das colunas com o 'printSchema()'

df_orders.printSchema()

# COMMAND ----------

# 5 - Verifcando os dados da coluna --> df[['name_column']]

display(df_orders[['order_amount']])

# COMMAND ----------

# 6 - Usando a função describe para verificar e detectar as anomalias 

# OBS: O describe() é parecido ao usar no SQL o ntitle


df_orders.describe().display()

# COMMAND ----------

'''

SQL ntitle() vs Pandas describe()

SELECT
  a.ntile,
  min(order_amount) as limite_inferior,
  max(order_amount) as limite_superior,
  avg(order_amount) as media,
  count(order_id) as orders
FROM
  (
    SELECT order_id, CAST(order_amount as FLOAT),
    ntile(4) OVER (ORDER BY CAST(order_amount as FLOAT)) as ntile
    FROM bronze.orders
  ) a
GROUP BY 1;


'''

# COMMAND ----------

# 7 - Verificando order_amount < 10000

# Ajuda a verificar esta anomalia para fazer o tratamento posterior

df_orders.filter(F.col('order_amount') < 10000).display()


# COMMAND ----------

# Importar bibliotecas

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# Função distinct()

# Verificar quais são os channel_id (Valores únicos) - 39 linhas

df_orders[['channel_id']].distinct().display()

# COMMAND ----------

# Função isin()

# 1, 10 e 11 --> São os 3 primeiros channel_id

# SQL = IN

# PANDAS = isin

df_orders_filter = df_orders[['order_id', 'store_id', 'order_amount', 'channel_id']].filter((F.col('channel_id').isin(['1', '10', '11']))).display()



# COMMAND ----------

# Fuções when() + otherwise()

# F.when() --> Deve ser utilizada apenas uma vez, após isso apenas .when() e .otherwise(), pois ele traz a 'F.' internamente 

# channel_name --> Nova coluna criada 

df_orders_filter \
    .withColumn('channel_name', F.when(F.col('channel_id') == '1', 'APP')
                .when(F.col('channel_id') == '10', 'SITE') \
                .otherwise('MARKET PLACE')
               ) \
.display()

# COMMAND ----------

# Juntando filter() + withColumn() + when() + otherwise()



df_all = df_orders[['order_id', 'store_id', 'order_amount', 'channel_id']].filter((F.col('channel_id').isin(['1', '10', '11'])))\
    .withColumn('channel_name', F.when(F.col('channel_id') == '1', 'APP')\
    .when(F.col('channel_id') == '10', 'SITE')\
    .otherwise('MARKET PLACE'))\
.display()


# COMMAND ----------

'''

USANDO SQL:


SELECT DISTINCT(channel_id) FROM bronze.orders;


SELECT
  order_id,
  store_id,
  order_amount,
  (
    CASE
      WHEN channel_id = "1" THEN "APP"
      WHEN channel_id = "10" THEN "SITE"
      ELSE "MARKET PLACE"
    END
  ) as channel
FROM
  bronze.orders
WHERE
  channel_id IN ("1", "10", "11");



'''
