# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## 1 - Entendimento de Negócio
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Food and Goods Deliveries in Brazil:**
# MAGIC
# MAGIC **O que é o Delivery Center:**
# MAGIC
# MAGIC Com seus diversos hubs operacionais espalhados pelo Brasil, o Delivery Center é uma plataforma integra lojistas e marketplaces, criando um ecossistema saudável para vendas de good (produtos) e food (comidas) no varejo brasileiro.
# MAGIC
# MAGIC Atualmente temos um cadastro (catálogo + cardápio) com mais de 900 mil itens, milhares de pedidos e entregas são operacionalizados diariamente com uma rede de milhares lojistas e entregadores parceiros espalhados por todas as regiões do país.
# MAGIC
# MAGIC Tudo isso gera dados e mais dados a todo momento!
# MAGIC
# MAGIC Diante disso, nosso negócio está cada vez data driven, ou seja, utilizando dados para tomar decisões e numa visão de futuro sabemos que utilizar os dados de forma inteligente pode ser o nosso grande diferencial no mercado.
# MAGIC
# MAGIC Este é o nosso contexto e com ele lhe propomos um desafio desafio em que você possa aplicar seus conhecimentos técnicos objetivando resolver problemas cotidianos de uma equipe de dados.
# MAGIC
# MAGIC https://www.kaggle.com/datasets/nosbielcs/brazilian-delivery-center
# MAGIC
# MAGIC
# MAGIC =================================================================================================================================================================================
# MAGIC
# MAGIC ##Descrição dos datasets:
# MAGIC
# MAGIC **Channels**: Este dataset possui informações sobre os canais de venda (marketplaces) onde são vendidos os good e food de nossos lojistas.
# MAGIC
# MAGIC **Deliveries**: Este dataset possui informações sobre as entregas realizadas por nossos entregadores parceiros.
# MAGIC
# MAGIC **Drivers**: Este dataset possui informações sobre os entregadores parceiros. Eles ficam em nossos hubs e toda vez que um pedido é processado, são eles fazem as entregas na casa dos consumidores.
# MAGIC
# MAGIC **Hubs**: Este dataset possui informações sobre os hubs do Delivery Center. Entenda que os Hubs são os centros de distribuição dos pedidos e é dali que saem as entregas.
# MAGIC
# MAGIC **Orders**: Este dataset possui informações sobre as vendas processadas através da plataforma do Delivery Center.
# MAGIC
# MAGIC **Payments**: Este dataset possui informações sobre os pagamentos realizados ao Delivery Center.
# MAGIC
# MAGIC **Stores**: Este dataset possui informações sobre os lojistas. Eles utilizam a Plataforma do Delivery Center para vender seus itens (good e/ou food) nos marketplaces.
# MAGIC
# MAGIC
# MAGIC =================================================================================================================================================================================
# MAGIC
# MAGIC
# MAGIC
# MAGIC ##Estrutura de Dados: 1 - Bronze, 2 - Prata, 3 - Ouro
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2 - Criação Banco de Dados - Camanda: Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Criando a camada Bronze
# MAGIC
# MAGIC CREATE DATABASE bronze;
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 3 - Criação e carregamento das Bases - Order + Stores

# COMMAND ----------

'''

# Pandas: 

import pandas as pd

df1 = pd.read_csv("/dbfs/FileStore/shared_uploads/1486559@sga.pucminas.br/stores.csv")
df2 = pd.read_csv("/dbfs/FileStore/shared_uploads/1486559@sga.pucminas.br/orders.csv")

'''


# Pyspark:

# Orders

df_orders = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/1486559@sga.pucminas.br/orders.csv")

df_stores = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/1486559@sga.pucminas.br/stores.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 4 - Convertendo os datasets para o formato "Delta" e salvando como "Tabela" na Base de Dados "Bronze"
# MAGIC

# COMMAND ----------

df_orders.write.format("delta").mode("overwrite").saveAsTable("bronze.orders")

df_stores.write.format("delta").mode("overwrite").saveAsTable("bronze.stores")

# COMMAND ----------

# Verificando as tabelas:

df_orders.display()

df_stores.display()

# COMMAND ----------

display(df_orders.printSchema())

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 5 - Trabalhando com SQL - Select, Where, Join e Limit

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM bronze.orders;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.orders WHERE order_status = 'FINISHED'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Dados únicos 
# MAGIC
# MAGIC SELECT DISTINCT(order_status) FROM bronze.orders

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.orders LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.stores LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT o.order_id, o.order_amount, s.store_name
# MAGIC FROM bronze.orders o  
# MAGIC JOIN bronze.stores s  
# MAGIC ON o.store_id = s.store_id
# MAGIC WHERE s.store_name = 'CUMIURI'
# MAGIC ORDER BY  o.order_amount DESC 
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 6 - Trabalhando com SQL - Cálculos Básicos e Agregações

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- order_delivery_fee  (Taxa de comissão da entrega)
# MAGIC -- order_delivery_cost (Custo da entrega)
# MAGIC
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   store_id,
# MAGIC   order_delivery_fee,
# MAGIC   order_delivery_cost,
# MAGIC   ROUND((order_delivery_fee + order_delivery_cost), 2) as total_cost -- Round(xxx,2) 2 digítos após a casa decimal
# MAGIC FROM
# MAGIC   bronze.orders
# MAGIC WHERE
# MAGIC   order_delivery_fee > 0  -- Para não vir dados zerados || order_delivery_fee != 0 ou order_delivery_fee > 0
# MAGIC AND
# MAGIC   order_delivery_cost IS NOT NULL -- Para não vir dados nulos 
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   store_id,
# MAGIC   SUM(order_amount) as TOTAL
# MAGIC FROM
# MAGIC   bronze.orders
# MAGIC WHERE
# MAGIC   store_id = '3512'
# MAGIC GROUP BY
# MAGIC   store_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   store_id,
# MAGIC   SUM(order_amount) as TOTAL
# MAGIC FROM
# MAGIC   bronze.orders
# MAGIC GROUP BY store_id
# MAGIC ORDER BY store_id ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC Select
# MAGIC  SUM(order_amount) as TOTAL
# MAGIC FROM
# MAGIC   bronze.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Verificar quantidade de lojas 
# MAGIC
# MAGIC SELECT COUNT(DISTINCT(store_id)) as QTD_LOJAS FROM bronze.stores;

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC -- Verificar quantidade de linhas de orders 
# MAGIC
# MAGIC SELECT COUNT(*) AS QTD_VENDAS FROM bronze.orders;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 7 - Tratando Dados Duplicados - Simulando duplicação e tratando os dados

# COMMAND ----------

# 1: Criando um novo dataframe atavés de um Select no Spark/Pyspark

orders2 = spark.sql("SELECT * FROM bronze.orders LIMIT 100")
display(orders2)

# COMMAND ----------

# 2: Duplicando os dados com o novo datraframe orders2 criado, aumentando em 100 linhas duplicadas no dataframe original 

orders2.write.format("delta").mode("append").saveAsTable("bronze.orders")

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC -- 3: Verificar quantidade de linhas de orders após simular os dados duplicados
# MAGIC
# MAGIC SELECT COUNT(*) AS QTD_VENDAS FROM bronze.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 4: Verificar quais são as order_id duplicadas
# MAGIC
# MAGIC
# MAGIC SELECT order_id, store_id, order_amount, COUNT(*) as qtd_registros_duplicados FROM bronze.orders GROUP BY order_id, store_id, order_amount;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 5: Verificar a quantidade de linhas duplicadas de order_id (SUBQUERIES - SUBSELECT)
# MAGIC
# MAGIC SELECT COUNT(*) AS Total_Duplicados
# MAGIC FROM(SELECT order_id, store_id, order_amount, COUNT(*) as qtd_registros_duplicados FROM bronze.orders GROUP BY order_id, store_id, order_amount) a
# MAGIC WHERE a.qtd_registros_duplicados > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 6: Criando um Select Distinct para trazer informações sem os dados duplicados
# MAGIC
# MAGIC
# MAGIC SELECT DISTINCT(*) FROM bronze.orders; -- 6.683 Dados sem duplicação 

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 8 - Criando a Base de Dados "PRATA"

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC CREATE DATABASE prata;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 9 - Criando tabela "orders" na Base de Dados "prata" vindo de um "Select Distinct"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Tabela prata.orders sem dados duplicados
# MAGIC -- Lembrando que é uma tabela delta, pois bronze.orders é uma tabela delta
# MAGIC
# MAGIC CREATE TABLE prata.orders as (SELECT DISTINCT(*) FROM bronze.orders);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) FROM prata.orders;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 10 - Detectando Anomalias

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM bronze.orders LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 1: Verificando anomalias de quartile com Ntile 
# MAGIC
# MAGIC -- OBS: CAST VS CONVERT 
# MAGIC -- Neste caso podemos analisar que no 4 Quartile houve uma anomalia(discrepânçia) no limite_superior
# MAGIC
# MAGIC SELECT
# MAGIC   a.ntile,
# MAGIC   min(order_amount) as limite_inferior,
# MAGIC   max(order_amount) as limite_superior,
# MAGIC   avg(order_amount) as media,
# MAGIC   count(order_id) as orders
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT order_id, CAST(order_amount as FLOAT),
# MAGIC     ntile(4) OVER (ORDER BY CAST(order_amount as FLOAT)) as ntile
# MAGIC     FROM bronze.orders
# MAGIC   ) a
# MAGIC GROUP BY 1; -- GROUP BY ntile == GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT order_id, CAST(order_amount as FLOAT),
# MAGIC     ntile(4) OVER (ORDER BY CAST(order_amount as FLOAT)) as ntile
# MAGIC     FROM bronze.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 2: Verificando quais orders são anomalias 
# MAGIC -- order_id = 84504844 e 93127697 (Anomalias)
# MAGIC
# MAGIC SELECT * FROM bronze.orders WHERE CAST(order_amount as FLOAT) > 100000;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 11 - Tratamento de Dados com Estrutura "CASE"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT(channel_id) FROM bronze.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /*
# MAGIC
# MAGIC Tem algumas formas de conseguir utilizar um range no Case When, se for sequencial você pode utilizar um Maior e Menor que:
# MAGIC
# MAGIC WHEN channel_id >= 1 AND channel_id <= 10  THEN "APP"
# MAGIC
# MAGIC Se não for sequencial você pode tentar utilizar um operador "IN"
# MAGIC
# MAGIC WHEN channel_id IN (1, 4, 7) THEN "APP"
# MAGIC
# MAGIC
# MAGIC */
# MAGIC
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   store_id,
# MAGIC   order_amount,
# MAGIC   (
# MAGIC     CASE
# MAGIC       WHEN channel_id = "1" THEN "APP"
# MAGIC       WHEN channel_id = "10" THEN "SITE"
# MAGIC       ELSE "MARKET PLACE"
# MAGIC     END
# MAGIC   ) as channel
# MAGIC FROM
# MAGIC   bronze.orders
# MAGIC WHERE
# MAGIC   channel_id IN ("1", "10", "11");

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 12 - Casting

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   CAST(order_amount AS FLOAT)
# MAGIC FROM
# MAGIC   bronze.orders
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Trabalhando com CAST + REPLACE + COLUNA FIXA DE EXEMPLO
# MAGIC -- Foi criado uma nova coluna preco_base para servir de exemplo do cast + replace
# MAGIC
# MAGIC SELECT
# MAGIC   CAST(order_amount AS FLOAT) AS preco_total,
# MAGIC   "R$ 19,95" AS preco_base,
# MAGIC   CAST(REPLACE(REPLACE(REPLACE("R$ 19,95", "R$", ""), " ", ""), ",", ".") AS FLOAT) AS preco_base_formatado
# MAGIC FROM
# MAGIC   bronze.orders
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Análise exploratória básica para entender a estrutura das datas de criação do pedido
# MAGIC  
# MAGIC SELECT
# MAGIC   order_created_day,
# MAGIC   order_created_month,
# MAGIC   order_moment_created
# MAGIC FROM
# MAGIC   bronze.orders
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- SUBSTRING(string, start, length)
# MAGIC -- TO_DATE(date_string,[format_convert]) (TO_DATE É UM CASTING)
# MAGIC
# MAGIC SELECT
# MAGIC   order_moment_created,
# MAGIC   TO_DATE(REPLACE(SUBSTRING(order_moment_created, 1, 9), " ", ""), "M/d/yyyy") AS order_moment_created_formated 
# MAGIC                                                                                                             -- Replace para tirar os espaços para não atrapalhar o Casting do TO_DATE
# MAGIC FROM                                                                                                        -- Tem de analisar qual formato é a base de dados, neste caso o mês é 'M' e não 'm'
# MAGIC   bronze.orders
# MAGIC WHERE
# MAGIC   order_created_day > 10
# MAGIC AND
# MAGIC   order_created_month > 2;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 13 - Dados Ausentes

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Verificando toda a tabela
# MAGIC
# MAGIC SELECT * FROM bronze.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Verificando quantas nulas 
# MAGIC
# MAGIC SELECT COUNT(*) FROM bronze.orders where order_delivery_cost is null;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Verificando quantas linhas não são nulas 
# MAGIC
# MAGIC SELECT COUNT(*) FROM bronze.orders where order_delivery_cost is not null;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Alterando valores nulos com "CASE"
# MAGIC -- Adicionando valor padrão
# MAGIC
# MAGIC
# MAGIC SELECT order_amount,
# MAGIC        (CASE
# MAGIC         WHEN order_delivery_cost IS NULL THEN 0
# MAGIC         ELSE order_delivery_cost
# MAGIC         END) as order_delivery_cost
# MAGIC FROM bronze.orders
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Alterando valores nulos com Tendência Central
# MAGIC -- Podemos usar uma tendência central, como Media (Menos robusta para Anomalias), Mediana (Mais robusta para Anomalias), Moda 
# MAGIC -- ROUND(number, decimals, operation)
# MAGIC
# MAGIC
# MAGIC SELECT
# MAGIC   order_amount,
# MAGIC   (
# MAGIC     CASE
# MAGIC       WHEN 
# MAGIC         order_delivery_cost IS NULL AND order_amount > 0 THEN ROUND((SELECT MEDIAN(order_delivery_cost) FROM bronze.orders),2)
# MAGIC       ELSE order_delivery_cost
# MAGIC     END
# MAGIC   ) AS order_delivery_cost
# MAGIC FROM
# MAGIC   bronze.orders
# MAGIC LIMIT 5;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   order_amount,
# MAGIC   (
# MAGIC     CASE
# MAGIC       WHEN 
# MAGIC         order_delivery_cost IS NULL AND order_amount > 0 THEN ROUND((SELECT AVG(order_delivery_cost) FROM bronze.orders),2)
# MAGIC       ELSE order_delivery_cost
# MAGIC     END
# MAGIC   ) AS order_delivery_cost
# MAGIC FROM
# MAGIC   bronze.orders
# MAGIC LIMIT 5;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 14 - Criando Tabela Final Tratada

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 1: Criando a tabela resumida (Este é o subselect)
# MAGIC
# MAGIC SELECT 
# MAGIC     TO_DATE(REPLACE(SUBSTRING(orders.order_moment_created, 1, 9), " ", ""), "M/d/yyyy") AS order_moment_date,
# MAGIC     stores.store_name,
# MAGIC     CAST(order_amount AS FLOAT) AS order_amount
# MAGIC   FROM
# MAGIC     bronze.orders
# MAGIC   INNER JOIN
# MAGIC     bronze.stores
# MAGIC   ON
# MAGIC     stores.store_id = orders.store_id
# MAGIC   WHERE
# MAGIC     order_moment_created IS NOT NULL
# MAGIC   AND
# MAGIC     CAST(order_amount AS FLOAT) <= 10000;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Criando a tabela Final
# MAGIC -- Esta tabela foi criada com base na regra de negócio pedida em que foi pedido o nome da loja, e soma de todas as vendas de todos os dias
# MAGIC
# MAGIC CREATE TABLE prata.orders_amount_store AS (SELECT
# MAGIC   order_moment_date,
# MAGIC   store_name,
# MAGIC   ROUND(SUM(order_amount), 2) AS total
# MAGIC FROM
# MAGIC   (SELECT 
# MAGIC     TO_DATE(REPLACE(SUBSTRING(orders.order_moment_created, 1, 9), " ", ""), "M/d/yyyy") AS order_moment_date,
# MAGIC     stores.store_name,
# MAGIC     CAST(order_amount AS FLOAT) AS order_amount
# MAGIC   FROM
# MAGIC     bronze.orders
# MAGIC   INNER JOIN
# MAGIC     bronze.stores
# MAGIC   ON
# MAGIC     stores.store_id = orders.store_id
# MAGIC   WHERE
# MAGIC     order_moment_created IS NOT NULL
# MAGIC   AND
# MAGIC     CAST(order_amount AS FLOAT) <= 10000)
# MAGIC GROUP BY
# MAGIC   order_moment_date, store_name);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Verificando a nova tabela criada prata.orders_amount_store 
# MAGIC
# MAGIC SELECT * FROM prata.orders_amount_store;

# COMMAND ----------


