# Implementação de Pipeline de Dados com AWS Glue

## Descrição do Projeto
Este repositório é dedicado à demonstração da construção de um pipeline de dados adaptado para o contexto da AWS, utilizando as tecnologias AWS Glue, S3, Glue Catalog, Spark, Python e Docker. O objetivo é proporcionar uma visão prática sobre como desenvolver e gerenciar pipelines de dados eficientes na nuvem da AWS.

## Pré-requisitos
Para seguir este guia, é necessário ter instalado em seu ambiente:

- Docker
- Python 3.10.12
- AWS CLI configurado com credenciais de acesso

## Instruções de Uso

### Configuração Inicial
- **Preparação do Ambiente AWS**: Antes de tudo, assegure-se de que suas credenciais da AWS estão configuradas corretamente. Isso inclui acesso ao AWS Glue, S3 e Glue Catalog.
  
- **Variáveis de Ambiente**: Configure as variáveis de ambiente necessárias para a execução dos scripts AWS Glue. Isso é feito no arquivo `.env`, baseando-se no modelo fornecido pelo arquivo `.env.example`. Este passo é crucial para garantir que o container Docker possa acessar os recursos da AWS corretamente.

### Inicialização do Ambiente de Desenvolvimento
- **Subindo o Container Docker**: Ao invés de utilizar um arquivo `docker-compose`, neste projeto utilizamos um script shell (`nome_do_script.sh`) para subir um container Docker. Este container é otimizado para o desenvolvimento de código AWS, contendo todas as ferramentas e bibliotecas necessárias. Execute o script no terminal para iniciar o container. Uma vez que o container está disponivel, configure o VSCode para acessar o ambiente de desenvolvimento de acordo com as instruções fornecidas nesse post (procurar o sub titulo 'Visual Studio Code'): https://aws.amazon.com/blogs/big-data/develop-and-test-aws-glue-version-3-0-jobs-locally-using-a-docker-container/ 

### Adicionando e Removendo Módulos Python
- **Gerenciamento de Módulos**: Uma vez no ambiente do container, você pode adicionar ou remover módulos Python utilizando os scripts `pip_install.sh` e `pip_remove.sh`, respectivamente. Esses scripts facilitam a gestão das dependências necessárias para o desenvolvimento dos códigos destinados ao AWS Glue.

### Desenvolvimento e Execução de Scripts AWS Glue
- **Código AWS Glue**: Todo o código foi adaptado para trabalhar com os serviços AWS Glue, S3 e Glue Catalog. Você encontrará os scripts dentro da pasta `src`, prontos para serem desenvolvidos e testados diretamente do container Docker.

- **Execução e Testes**: Após o desenvolvimento, você pode testar seus scripts AWS Glue diretamente no ambiente configurado. É importante seguir as melhores práticas da AWS para a execução e gerenciamento de jobs do Glue.

Seguindo estas instruções, você estará apto a configurar, desenvolver e gerenciar pipelines de dados na AWS, utilizando as poderosas ferramentas que a plataforma oferece.