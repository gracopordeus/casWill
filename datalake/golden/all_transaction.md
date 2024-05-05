# Documentação do Script de Manipulação e Processamento de Dados

## Visão Geral

Este script tem como objetivo principal manipular e processar dados relacionados a transações PIX e informações de clientes. Ele realiza uma série de operações, incluindo a criação de tabelas, visualizações temporárias, inserção de dados e exportação para um arquivo Parquet. O script utiliza variáveis de ambiente para configuração e manipula datas para gerar nomes de arquivos.

## Variáveis de Ambiente

O script utiliza as seguintes variáveis de ambiente:

- `MINIO_GOLDEN`: Caminho para o diretório onde os arquivos Parquet serão exportados.

Certifique-se de configurar corretamente a variável de ambiente `MINIO_GOLDEN` antes de executar o script.

## Manipulação de Datas

O script utiliza a biblioteca `datetime` para manipular datas. A data atual é obtida usando `datetime.now()` e formatada como uma string no formato 'YYYY-MM-DD' usando o método `strftime()`. Essa data formatada é usada para gerar o nome do arquivo Parquet exportado.

## Operações SQL

O script executa as seguintes operações SQL:

1. Criação da tabela `golden.all_transaction`:
   - A tabela é criada se ainda não existir.
   - A tabela possui colunas para armazenar informações sobre transações PIX e dados dos clientes.
   - A coluna `transaction_id` é definida como chave primária.

2. Criação da visualização temporária `temp_view_golden_all_transaction`:
   - A visualização temporária é criada usando a cláusula `CREATE OR REPLACE TEMP VIEW`.
   - A visualização combina dados das tabelas `silver.pix_transaction` e `silver.customer` usando um JOIN baseado na coluna `customer_id`.
   - A visualização seleciona colunas específicas das tabelas base.

3. Inserção de dados na tabela `golden.all_transaction`:
   - Os dados são inseridos na tabela `golden.all_transaction` a partir da visualização temporária `temp_view_golden_all_transaction`.

4. Exportação dos dados para um arquivo Parquet:
   - Os dados da tabela `golden.all_transaction` são exportados para um arquivo Parquet usando a cláusula `COPY`.
   - O arquivo Parquet é salvo no diretório especificado pela variável de ambiente `MINIO_GOLDEN`, com o nome de arquivo gerado a partir da data atual.

## Comentários no Código

O script inclui comentários para a tabela `golden.all_transaction` e suas colunas. Esses comentários fornecem uma descrição detalhada do propósito da tabela e o significado de cada coluna. Os comentários são aplicados usando as instruções `COMMENT ON TABLE` e `COMMENT ON COLUMN`.

## Exemplos de Uso

Para usar o script, siga estas etapas:

1. Configure a variável de ambiente `MINIO_GOLDEN` com o caminho desejado para exportar os arquivos Parquet.

2. Execute o script usando um interpretador Python compatível.

Exemplo de comando para executar o script:

```bash
python nome_do_script.py
```

O script irá criar a tabela `golden.all_transaction`, inserir os dados combinados das tabelas `silver.pix_transaction` e `silver.customer`, e exportar os dados para um arquivo Parquet no diretório especificado.

## Conclusão

A tabela `golden.all_transaction` possui o seguinte esquema:

```
transaction_id (VARCHAR): Identificador único da transação PIX.
transaction_date (DATE): Data da transação.
transaction_month (INTEGER): Mês numérico da transação.
customer_id (INTEGER): Identificador único do cliente.
cd_seqlan (INTEGER): Código sequencial do lançamento da transação.
transaction_type (VARCHAR): Tipo da transação (PIX ou outro).
transaction_value (DOUBLE): Valor monetário da transação.
birth_date (DATE): Data de nascimento do cliente.
uf_name (VARCHAR): Nome do estado de residência do cliente.
uf (VARCHAR): Sigla do estado de residência do cliente.
year (INTEGER): Partição de Ano da data da transação.
month (INTEGER): Partição de Mês da data da transação.
day (INTEGER): Partição de Dia da data da transação.
```

Essa tabela fornece uma visão abrangente das transações PIX, combinando informações das transações com dados detalhados dos clientes. Ela pode ser usada para análises e relatórios relacionados às atividades financeiras dos clientes envolvendo transações PIX.