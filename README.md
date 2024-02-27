# Implementação de Pipeline de Dados com PySpark
## Descrição do Projeto
Este repositório é dedicado à demonstração da construção de um pipeline de dados utilizando as tecnologias Spark, Python e Docker.

## Pré-requisitos
Para seguir este guia, é necessário ter instalado em seu ambiente:

    - Docker
    - Docker Compose
    - Python 3.10.12
    - Poetry (opcional, mas recomendado)

## Instruções de Uso

- Configuração Inicial: Comece pela configuração das variáveis de ambiente. Isso é feito no arquivo .env, baseando-se no modelo fornecido pelo arquivo .env.example. É importante destacar que o docker-compose.yml já está preparado para utilizar as variáveis de ambiente definidas no arquivo .env, eliminando a necessidade de ajustes adicionais neste arquivo.

- Inicialização dos Serviços: Com as variáveis de ambiente devidamente configuradas no arquivo .env, inicie os serviços necessários, incluindo o Spark e o MSSQL, executando o comando docker-compose up no terminal, diretamente na pasta raiz do projeto.

- Criação de Tabelas: Após a inicialização dos serviços, acesse o banco de dados e execute o script disponível em "db/create_tables.sql" para criar as tabelas necessárias para o funcionamento do pipeline.

- Execução de Scripts: Com os serviços rodando e o arquivo .env configurado, você está pronto para executar os scripts localizados na pasta "src". A execução pode ser realizada por meio do Jupyter Notebook, utilizando o VSCode para uma experiência integrada.

Seguindo estas instruções, você estará apto a configurar e operar o pipeline de dados proposto.


# TODO

- [x] Adicionar chaves estrangeiras e índices às tabelas do banco de dados
- [ ] Configurar o projeto para usar a imagem do AWS Glue no Docker para desenvolvimento local
- [ ] Criar buckets no S3 para armazenar os dados e criar as tabelas no Athena utilizando o padrão Apache Iceberg
- [ ] Converter os scripts desenvolvidos para AWS Glue
- [ ] Criar e agendar a execução de jobs no AWS Glue usando StepFunctions
- [ ] Reescrever os scripts PySpark em AWS Glue Python Shell
