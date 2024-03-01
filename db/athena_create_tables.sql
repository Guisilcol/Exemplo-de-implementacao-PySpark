CREATE DATABASE compras;

CREATE TABLE compras.cep (
  CEP int,
  UF string,
  CIDADE string,
  BAIRRO string,
  LOGRADOURO string
)
LOCATION 's3://repositorio-implementacao-pyspark/databases/compras/cep'
TBLPROPERTIES (
  'table_type'='iceberg',
  'write_compression'='GZIP',
  'format'='parquet'
);

CREATE TABLE compras.condicao_pagamento (
  ID_CONDICAO int,
  DESCRICAO string,
  QTD_PARCELAS int
)
LOCATION 's3://repositorio-implementacao-pyspark/databases/TCS/condicao_pagamento'
TBLPROPERTIES (
  'table_type'='iceberg',
  'write_compression'='GZIP',
  'format'='parquet'
);

CREATE TABLE compras.enderecos_fornecedores (
  ID_ENDERECO_FORNECEDOR int,
  CEP int,
  ID_FORNECEDOR int,
  ID_TIPO_ENDERECO int,
  NUM_ENDERECO string,
  COMPLEMENTO string
)
LOCATION 's3://repositorio-implementacao-pyspark/databases/TCS/enderecos_fornecedores'
TBLPROPERTIES (
  'table_type'='iceberg',
  'write_compression'='GZIP',
  'format'='parquet'
);

CREATE TABLE compras.fornecedores (
  ID_FORNECEDOR int,
  NOME string,
  CNPJ string,
  EMAIL string,
  TELEFONE string
)
LOCATION 's3://repositorio-implementacao-pyspark/databases/TCS/fornecedores'
TBLPROPERTIES (
  'table_type'='iceberg',
  'write_compression'='GZIP',
  'format'='parquet'
);

CREATE TABLE compras.notas_fiscais_entrada (
  ID_NF_ENTRADA int,
  NUMERO_NF string,
  ID_FORNECEDOR int,
  ID_CONDICAO int,
  DATA_EMISSAO date,
  VALOR_NET decimal(10,2),
  VALOR_TRIBUTO decimal(10,2),
  VALOR_TOTAL decimal(10,2),
  NOME_ITEM string,
  QTD_ITEM int
)
LOCATION 's3://repositorio-implementacao-pyspark/databases/TCS/notas_fiscais_entrada'
TBLPROPERTIES (
  'table_type'='iceberg',
  'write_compression'='GZIP',
  'format'='parquet'
);

CREATE TABLE compras.programacao_pagamento (
  ID_PROG_PAGAMENTO int,
  ID_NF_ENTRADA int,
  DATA_VENCIMENTO date,
  NUM_PARCELA int,
  VALOR_PARCELA decimal(21,13),
  STATUS_PAGAMENTO string
)
LOCATION 's3://repositorio-implementacao-pyspark/databases/TCS/programacao_pagamento'
TBLPROPERTIES (
  'table_type'='iceberg',
  'write_compression'='GZIP',
  'format'='parquet'
);

CREATE TABLE compras.tipo_endereco (
  ID_TIPO_ENDERECO int,
  DESCRICAO string,
  SIGLA string
)
LOCATION 's3://repositorio-implementacao-pyspark/databases/TCS/tipo_endereco'
TBLPROPERTIES (
  'table_type'='iceberg',
  'write_compression'='GZIP',
  'format'='parquet'
);
