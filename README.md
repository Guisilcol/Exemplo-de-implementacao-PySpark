# Implementação de Pipelines de Dados com AWS Glue 

Este repositório é dedicado à implantação do código fonte disponivel no branch 'convetional-cluster' desse repositório no AWS Glue, preparando desde o ambiente de desenvolvimento até a implantação dos JOB's na AWS. 

## Pré-requisitos

- Conta na AWS configurada na máquina
- VS Code 
    - Extensão 'Dev Container' da Microsoft disponivel na aba 'Extensions' do VS Code
    - Extensão 'Jupyter' da Microsoft disponivel na aba 'Extensions' do VS Code
- Docker 

## Estrutura do repositorio

A seguir, uma breve explicação das pastas e arquivos presentes no repositório:

- **data**: Pasta que contém os dados de entrada e saída do pipeline. Os arquivos presentes nessa pasta serão disponibilizados em um Bucket no S3.
- **db**: Pasta que contém os scripts de criação de tabelas no Glue Data Catalog. Todas as tabelas usadas no projeto são do tipo Iceberg.
- **sh**: Pasta que contém os scripts shell que auxiliam o desenvolvimento. 
- **src**: Pasta que contém os códigos fonte do pipeline. Todos os arquivos presentes nessa pasta podem ser considerados individualmente como JOB's do Glue. 
    - **external_libs**: Pasta que armazena as bibliotecas externas que são utilizadas nos JOB's do Glue. Todos os jobs usam, ao menos, um modúlo presente nessa pasta.

- ***.env.example***: Arquivo que contém as variáveis de ambiente que são utilizadas no projeto. Esse arquivo deve ser copiado para um arquivo .env e preenchido com as informações corretas.
- ***start_dev_container.bat/start_dev_container.sh***: Script que inicia o ambiente de desenvolvimento subindo um container utilizando Docker. Esse script deve ser executado na raiz do projeto.

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

9. Com o ambiente de desenvolvimento e AWS configurados, devemos alterar as variáveis de ambiente do projeto. Copie o arquivo `.env.example` para um arquivo chamado `.env` e preencha as variáveis de ambiente com as informações corretas, como segue o exemplo a seguir: 

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

10. Com as variáveis de ambiente configuradas, você pode começar a desenvolver os JOB's do Glue.

## Deploy no AWS Glue
...