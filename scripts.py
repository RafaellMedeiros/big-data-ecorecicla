# Os script foram executados utilizando o Colab do Google

# inicialmente foi instalado o framework pyspark
# !pip install pyspark

# Realizado a configuração inicial do sparkSession

# Inicializar SparkSession
spark = SparkSession.builder \
    .appName("RecyclingDataAnalysis") \
    .getOrCreate()

# Importar as bibliotecas necessárias
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession

# Instalar e importar o PySpark
!pip install pyspark

# Inicializar SparkSession
spark = SparkSession.builder \
    .appName("RecyclingDataAnalysis") \
    .getOrCreate()

# Pegar dados
df = pd.read_csv('/path/to/dados_reciclagem.csv')

# Criar DataFrame do Spark
spark_df = spark.createDataFrame(df)

# Mostrar a estrutura do DataFrame
spark_df.printSchema()

# Mostrar as primeiras linhas do DataFrame
spark_df.show()

# Calcular a média de eficiência das rotas
spark_df.select('Eficiência_Rota (%)').groupBy().mean().show()

# Variabilidade dos valores dos produtos
spark_df.groupBy('Material').agg({'Valor_Produto (R$/kg)': 'stddev'}).show()

# Correlação entre quantidade reciclada e demanda de venda
correlation = spark_df.stat.corr('Quantidade_Reciclada (kg)', 'Demanda_Venda (kg)')
print(f"Correlação entre Quantidade Reciclada e Demanda de Venda: {correlation:.2f}")

import matplotlib.pyplot as plt

# Gráfico de Quantidade Reciclada por Material
plt.figure(figsize=(10, 6))
df.groupby('Material')['Quantidade_Reciclada (kg)'].sum().plot(kind='bar', color='skyblue')
plt.title('Quantidade Reciclada por Tipo de Material')
plt.xlabel('Material')
plt.ylabel('Quantidade Reciclada (kg)')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Gráfico de Eficiência das Rotas ao Longo do Tempo
plt.figure(figsize=(10, 6))
plt.plot(df['Data'], df['Eficiência_Rota (%)'], marker='o', linestyle='-', color='green')
plt.title('Eficiência das Rotas ao Longo do Tempo')
plt.xlabel('Data')
plt.ylabel('Eficiência da Rota (%)')
plt.grid(True)
plt.tight_layout()
plt.show()

# Gráfico de Valor dos Produtos por Tipo de Material
plt.figure(figsize=(10, 6))
df.groupby('Material')['Valor_Produto (R$/kg)'].mean().plot(kind='bar', color='orange')
plt.title('Valor Médio dos Produtos por Tipo de Material')
plt.xlabel('Material')
plt.ylabel('Valor Médio (R$/kg)')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Gráfico de Correlação entre Quantidade Reciclada e Demanda de Venda
plt.figure(figsize=(10, 6))
plt.scatter(df['Quantidade_Reciclada (kg)'], df['Demanda_Venda (kg)'], color='purple')
plt.title('Correlação entre Quantidade Reciclada e Demanda de Venda')
plt.xlabel('Quantidade Reciclada (kg)')
plt.ylabel('Demanda de Venda (kg)')
plt.grid(True)
plt.tight_layout()
plt.show()