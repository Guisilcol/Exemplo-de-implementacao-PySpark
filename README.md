# Implementação de Pipelines de Dados com AWS Glue

Este repositório concentra-se em orientar o usuário na implantação do código-fonte que se encontra no branch 'convetional-cluster' no serviço AWS Glue. O processo abrange desde a configuração inicial do ambiente de desenvolvimento até a efetiva implantação dos jobs no AWS Glue.

## Pré-requisitos

Para começar, você precisará ter:

- Uma conta AWS já configurada na sua máquina.
- O Visual Studio Code (VS Code) instalado, junto com as seguintes extensões disponíveis na aba 'Extensões':
    - 'Dev Container' da Microsoft, para trabalhar com containers de desenvolvimento.
    - 'Jupyter' da Microsoft, para suporte a notebooks Jupyter.
- Docker instalado, para criar e gerenciar containers.

## Estrutura do repositorio

A seguir, uma breve explicação das pastas e arquivos presentes no repositório:

- data: Esta pasta armazena os dados que serão processados pelo pipeline. Ela inclui tanto os dados de entrada quanto os resultados de saída. Esses dados serão posteriormente armazenados em um Bucket no Amazon S3.
- db: Aqui, você encontra os scripts necessários para criar tabelas no Glue Data Catalog. Importante ressaltar que todas as tabelas criadas para este projeto são do tipo Iceberg, uma estrutura de dados otimizada para grandes conjuntos de dados que possibilitam o uso de operações ACID.
- sh: Esta pasta contém scripts em Shell que facilitam diversas tarefas de desenvolvimento (como adicionar novas pacotes no Python)
- src: Dentro desta pasta, estão localizados os códigos-fonte específicos do pipeline. Cada arquivo aqui pode ser tratado como um job individual no AWS Glue.
- external_libs: Subpasta que guarda as bibliotecas externas necessárias para os jobs do Glue. É garantido que cada job irá requerer pelo menos uma biblioteca encontrada aqui.
- .env.example: Um arquivo de exemplo que lista todas as variáveis de ambiente necessárias para o projeto. Você deve copiá-lo para um novo arquivo chamado .env e preencher as variáveis com as informações apropriadas.
- start_dev_container.bat/start_dev_container.sh: Scripts para inicializar o ambiente de desenvolvimento por meio do Docker. Esses scripts devem ser executados na raiz do projeto para subir um container, preparando o ambiente para o desenvolvimento.

## Preparando o ambiente AWS 

Para preparar o ambiente AWS, siga os passos a seguir:

1. Crie um novo Bucket no S3. Será usado para armazenar os dados de entrada, dados das tabelas do Glue Catalog, dados de arquivos stage, código-fonte dos JOB's e consultas do Athena. Nesse exemplo, considere que o nome do Bucket é `repositorio-implementacao-pyspark`.

2. Crie as seguintes pastas no Bucket criado na etapa anterior:
    - `databases`: Pasta que armazena os dados de entrada e saída do pipeline.
    - `input_files`: Pasta que armazena os scripts de criação de tabelas no Glue Data Catalog.
    - `work_files`: Pasta que armazena os dados de arquivos stage.
    - `src_code`: Pasta que armazena os códigos fonte do pipeline.
    - `athena`: Pasta que armazena as consultas do Athena.
    - `temp_glue_assets`: Pasta que armazena arquivos temporários da execução dos Glue Jobs. 

3. Faça upload de todos arquivos disponiveis na pasta `data` para a pasta `input_files` do Bucket. 

4. Abra o Athena e, quando for solicitado, informe o diretório da pasta `athena` do seu Bucket para que consultas sejam armazenadas (por exemplo: `s3://repositorio-implementacao-pyspark/athena/`). Execute as declarações SQL do arquivo `db/athena_create_tables.sql` para criar as tabelas e o banco de ados no Athena. Lembre-se que cada declaração SQL deve ser executada de forma individual, por tanto, execute uma declaração por vez.

## Preparando o ambiente de desenvolvimento

Para preparar o ambiente de desenvolvimento, siga os passos a seguir:

1. Certifique-se de que todos pré-requisitos estão instalados e funcionais na máquina. 

2. Clone o repositório em sua máquina.

3. Editar o arquivo start_dev_container.sh (caso seu sistema operacional seja Windows, edite o arquivo start_dev_container.bat) e substituir o valor 'DIRETORIO_AWS' pelo caminho do diretório onde estão as credenciais da AWS (pasta .aws)

4. Executar o script alterado na etapa anterior. Esse script irá subir um container com o ambiente de desenvolvimento configurado. Lembre-se de executar ele usando seu terminal estando na raiz do projeto.

5. Após a execução do script, o ambiente de desenvolvimento estará pronto para ser utilizado e você poderá acessá-lo através do VS Code usando a extensão 'Dev Container' da Microsoft. Na aba 'Remote Explorer', procure pelo container 'amazon/aws-glue-libs:glue_libs_4.0.0_image_01' e clique em 'Attach in Current Window'.
    - O VSCode precisa de uma configuração especifica para funcionar corretamente. Essa configuração já esta disponivel na pasta `.vscode`

6. Uma vez no ambiente de desenvolvimento, certifique-se de instalar suas extensões preferidas. Recomendo A extensao 'Python' e 'Jupyter' da Microsoft. 

7. Para testar o ambiente, crie um novo arquivo chamado `teste.py` na pasta `src` e insira o seguinte código:

```python

#%% Importing the libraries
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

#%% Initialize the SparkSession

SPARK_CONTEXT = SparkContext().getOrCreate()
GLUE_CONTEXT = GlueContext(SPARK_CONTEXT)
SPARK = GLUE_CONTEXT.spark_session
JOB = Job(GLUE_CONTEXT)

#%% Load the function to compute dataframes

def compute_dataframe_with_spark_session(spark: SparkSession) -> DataFrame:
    """Creates a dataframe with the spark session"""
    
    # Create a dataframe
    df = spark.createDataFrame(
        [
            (1, "John", "Doe"),
            (2, "Anna", "Smith"),
            (3, "Peter", "Jones")
        ], ("id", "first_name", "last_name"))
    
    return df

def compute_dynamic_frame_with_glue_context(glue_context: GlueContext) -> DynamicFrame:
    """Creates a DynamicFrame with the glue context"""
    
    # Create a dynamic frame
    dyf: DynamicFrame = glue_context.create_dynamic_frame.from_rdd(
        [
            (1, "John", "Doe"),
            (2, "Anna", "Smith"),
            (3, "Peter", "Jones")
        ],
        'dfy'
    )
    
    dyf = (
        dyf
        .rename_field('_1', 'id')
        .rename_field('_2', 'first_name')
        .rename_field('_3', 'last_name')
    )
    return df 


## Execution

if __name__ == "__main__":
    print('Dataframe with SparkSession: ')
    df = compute_dataframe_with_spark_session(SPARK)
    df.show()
    
    print('DynamicFrame with GlueContext: ')
    dyf = compute_dynamic_frame_with_glue_context(GLUE_CONTEXT)
    dyf.show()

```

8. Certifique-se que está usando a versão do Python 3.10.2 disponivel no container. Execute o código acima utilizando a opção 'Run Below' na primeira célula disponivel. Se tudo estiver configurado corretamente, você verá os resultados do código no terminal Jupyter que automaticamente será aberto.
    - ![image](https://github.com/Guisilcol/Exemplo-de-implementacao-PySpark/assets/40049979/f725f2de-4a45-47d0-ba64-79d6289ded6e)


10. Com o ambiente de desenvolvimento e AWS configurados, devemos alterar as variáveis de ambiente do projeto. Copie o arquivo `.env.example` para um arquivo chamado `.env` e preencha as variáveis de ambiente com as informações corretas, como segue o exemplo a seguir: 

    ```
    SOURCE_PATH=s3://repositorio-implementacao-pyspark/input_files
    WORK_PATH=s3://repositorio-implementacao-pyspark/work_files
    AWS_ACCESS_KEY_ID=
    AWS_SECRET_KEY=
    AWS_REGION=us-east-1
    AWS_DEFAULT_REGION=us-east-1
    AWS_WAREHOUSE=s3://repositorio-implementacao-pyspark
    ```

    Caso você não tenha a conta AWS configurada na máquina, você pode obter as credenciais de acesso no console da AWS e preencher as variáveis `AWS_ACCESS_KEY_ID` e `AWS_SECRET_KEY` com as informações corretas.

11. Com as variáveis de ambiente configuradas, você pode começar a desenvolver os JOB's do Glue.

12. Agora criaremos uma ROLE no IAM para possibilitar a execucação dos JOB's. Acesse o IAM via Console da AWS
    - ![image](https://github.com/Guisilcol/Exemplo-de-implementacao-PySpark/assets/40049979/2aef9915-d765-4126-98da-7b681058ee48)

13. No menu lateral, clique em "Roles"
    - ![image](https://github.com/Guisilcol/Exemplo-de-implementacao-PySpark/assets/40049979/8fc4c717-72b7-4be3-9306-6e707ffbc9de)

14. Clique em "Create Role": 
    - ![image](https://github.com/Guisilcol/Exemplo-de-implementacao-PySpark/assets/40049979/613810a6-ea5b-46a3-8a0a-661c71f64013)

15. Siga as seguintes configurações para a criação da Role:
    - ![image](https://github.com/Guisilcol/Exemplo-de-implementacao-PySpark/assets/40049979/7d389668-9dac-4c58-b429-eec48384b503)
    - ![image](https://github.com/Guisilcol/Exemplo-de-implementacao-PySpark/assets/40049979/6a0cba6f-2fdb-4ecc-bb1b-8dffb58500c9)
    - ![image](https://github.com/Guisilcol/Exemplo-de-implementacao-PySpark/assets/40049979/de1ea84f-90a2-45ae-8294-a1a2c528af2e)


## Deploy no AWS Glue (manualmente)

Uma vez que o código PySpark foi desenvolvido, poderemos fazer um deploy manual na AWS. O passo-a-passo a seguir mostra como fazer isso, fazendo deploy do job "AWS_GLUE_SPARK_JOB_tbl_cep.py" disponivel nesse repositório em `src/`

1. Acesse o serviço AWS Glue no console da AWS
    - ![image](https://github.com/Guisilcol/Exemplo-de-implementacao-PySpark/assets/40049979/4240d31e-e69e-4d6d-8618-cf38687ec534)
2. No menu lateral, acesse a aba "ETL Jobs"
    - ![image](https://github.com/Guisilcol/Exemplo-de-implementacao-PySpark/assets/40049979/d58f53af-f58e-44f5-80e8-6fd1c38f8a6f)
3. Na sessão de "Create job", clique na opção "Script editor" (no print abaixo é a última opção no lado direito)
    - ![image](https://github.com/Guisilcol/Exemplo-de-implementacao-PySpark/assets/40049979/edc04165-85cb-43a2-8bcc-70a282b88129)
4. Será aberta uma caixa de diálogo. Escolha as seguintes configurações (faça upload do seu arquivo .py) e clique em "Create Script":
    - ![image](https://github.com/Guisilcol/Exemplo-de-implementacao-PySpark/assets/40049979/c6ec0a95-8ccd-4d1f-9e0f-40f6b280a2c1)
5. Renomeie o seu JOB
    - ![image](https://github.com/Guisilcol/Exemplo-de-implementacao-PySpark/assets/40049979/4ab491e6-f1e2-469e-9427-376cbf370648)
6. Clique a aba "Job Details" e configure de acordo com os prints a seguir:
    - ![image](https://github.com/Guisilcol/Exemplo-de-implementacao-PySpark/assets/40049979/d40deca0-7830-44d2-8e28-4d020b240c37)
    - ![image](https://github.com/Guisilcol/Exemplo-de-implementacao-PySpark/assets/40049979/2d1bb4b9-5356-40a1-886d-15dd64e6688b)
    - ![image](https://github.com/Guisilcol/Exemplo-de-implementacao-PySpark/assets/40049979/edffdf15-d3ac-41f2-b4f5-e6801d91db98)
    - ![image](https://github.com/Guisilcol/Exemplo-de-implementacao-PySpark/assets/40049979/1de6bc89-c6a2-43d5-9819-037939f45c63)
        - Lembre-se que os parametros --AWS_WAREHOUSE, --SOURCE_PATH e --WORK_PATH estão presentes no arquivo .env. Coloque os mesmos valores do arquivo .env aqui. 
7. Após fazer todas as configurações, clique em "Save" no canto superior direito da tela.
8. Uma vez configurado, certifique-se que todos os arquivos de origem estão disponibilizados no S3. Para testar o JOB, clique me "Run" no canto superior direito da tela. Para acompanhar a sua execução, clique na aba "Runs". Caso o JOB e o ambiente tenham sido configurados da forma correta, o status será "Succeeded" 
    - ![image](https://github.com/Guisilcol/Exemplo-de-implementacao-PySpark/assets/40049979/d998b48d-d561-4b96-9087-5e885c322add)
9. Abra o serviço "Step Functions" via console da AWS
    - ![image](https://github.com/Guisilcol/Exemplo-de-implementacao-PySpark/assets/40049979/b51d380b-986a-4a21-9f4b-19374bf1868c)
10. Clique em "Create State Machine" e selecione o template "Blank"
    - ![image](https://github.com/Guisilcol/Exemplo-de-implementacao-PySpark/assets/40049979/34b8294e-ad01-40af-a53e-3d0a79b994dc)
    - ![image](https://github.com/Guisilcol/Exemplo-de-implementacao-PySpark/assets/40049979/ca9ad7b0-2cba-40e3-9a80-a046c8f66932)
11. Renomeie sua State machine da forma que preferir
12. Arraste um "Step" disponivel no menu do lado direito chamado "Glue StartJobRun"
13. Clique no Step e altere o API Parameter, adicionando o nome do JOB criado anteriormente e ativando a opção "Wait for task to complete - optional", conforme imagem abaixo
    - ![image](https://github.com/Guisilcol/Exemplo-de-implementacao-PySpark/assets/40049979/9844e119-9a1b-43db-ab8f-903d452069da)
14. Clique em "Create" no canto superior direito
15. Por fim, clique em "Start execution" e acompanhe a execução do JOB.
16. O deploy foi concluido com sucesso! Agora você pode fazer o deploy de outros JOBs e orquestra-los via Step Functions.



