# Anotações 

Origem: Arquivo CSV contendo todos os dados de todas as tabelas modeladas 

# Fluxo 

## Carga nas tabelas do BD a partir do CSV 

### Resumo:
    - Upsert na tabela de Notas Fiscais Entrada (No de VENDAS é SAIDA) 
    - Upsert na tabela de clientes
    - Upsert na tabela Condição de pagamento 
    - Upsert na tabela de Cep
    - Upsert na tabela Tipo Endereço
    - Carga em arquivo das linhas rejeitadas 

### Tratamentos do CSV: 
    - Tratar espaços em branco de todas as colunas X
    - Remover duplicatas X
    - Remover linhas com alguma coluna nula (com exceção da coluna complemento) X
    - Substituir os nulos da coluna complemento por "N/A" X
    - Tratar a coluna NOME_CLIENTE (remover espacos em branco no começo e fim e deixar o conteúdo em maiusculo) X
    - Fazer um tratamento na coluna CONDICAO_PAGAMENTO (texto livre) de forma que o conteudo seja unificado X
    - Tratar a coluna CNPJ Cliente (precisa ser um CNPJ válido) X
    - Tratar a coluna CEP (precisa ser um CEP válido) X
    - Caso a venda (nota fiscal) já existir no banco, a mesma deve ser rejeitada e salva em um arquivo de rejeição

## Carga na tabela de Programação de Pagamento (no de vendas é Recebimento)

### Resumo:
    - Upsert na tabela de Programação de Pagamento
    - Carga em arquivo das linhas rejeitadas

### Tratamentos:    
    - Join entre a tabela de Notas Fiscais e a tabela de condição de pagamento
    - Aplicar um explode na tabela resultante de forma que cada linha represente uma parcela 


## Carga na tabela de historico de pagamentos efetuados (no de vendas HISTORICO_RECEBIMENTO)


    


