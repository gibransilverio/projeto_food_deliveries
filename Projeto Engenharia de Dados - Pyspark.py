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


