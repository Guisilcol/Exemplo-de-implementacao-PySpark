# TCS-Exemplo-de-implementacao-PySpark

## Descrição

Este repositório contém um exemplo de implementação de um pipeline de dados utilizando as ferramentas Spark, Python e Docker. 

## Pré-requisitos

- Docker
- Docker Compose
- Python 3.10.12
- Recomendado: Poetry

## Utilização 

Configure as variáveis de ambiente no arquivo .env de acordo com o arquivo .env.example e configure os volumes e variaveis de ambiente no arquivo docker-compose.yml de acordo com suas alterações no .env. Recomendo utilizar diretórios absolutos nos valores das variáveis de ambiente do arquivo .env e docker-compose.yml.
Suba o serviço do Spark e MSSQL usando o comando 'docker-compose up' na raiz do projeto. 
Se conecte no banco de dados e execute o script de criação de tabelas localizado na pasta "db/create_tables.sql".
Uma vez que o serviço do Spark estiver rodando e o arquivo .env configurado, pode executar os scripts da pasta "src".