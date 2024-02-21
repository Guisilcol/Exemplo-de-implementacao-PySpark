
# TCS-Exemplo-de-implementacao-PySpark

## Descrição

Este repositório contém um exemplo de implementação de um pipeline de dados utilizando as ferramentas Spark, Python e Docker. 

## Pré-requisitos

- Docker
- Docker Compose
- Python 3.10

## Utilização 

Primeiro, suba o serviço do Spark usando o comando 'docker-compose up' na raiz do projeto. 
Agora, configure o arquivo .env com as variáveis de ambiente necessárias de acordo com o arquivo .env.example, colocando as credenciais do seu banco de dados e o diretório de entrada e saida dos arquivos (no caso, é o diretório da pasta 'data' na raiz do projeto).
Uma vez que o serviço do Spark estiver rodando e o arquivo .env configurado, pode executar os scripts da pasta "src/spark_code".