# Bem vindo à resuolução do case para AE SR!
Abordaremos as seguintes temáticas:
* Criação de um datalake (Bronze > Silver > Gold)
* Manipulação e orquestração de dados com SQL, DBT e Airflow;
* Observabilidade e alertas automatizados;
* Alert Sending;

"E lá vamos nóooos!"


## Tables Definition

### Table: `core_account`
| Column Name          | Type       | Description                      |
|----------------------|------------|----------------------------------|
| `id_transaction`     | varchar    | Identificador único da transação (primary key) |
| `dt_transaction`     | date       | Data de movimentação             |
| `dt_month`           | date       | Mês-ano da movimentação          |
| `cd_account_customer`| integer    | Número da conta do cliente       |
| `cd_seqlan`          | integer    | Código sequencial de transações  |
| `ds_transaction_type`| varchar    | Tipo da transação                |
| `vl_transaction`     | double     | Valor da transação               |

### Table: `core_pix`
| Column Name          | Type       | Description                      |
|----------------------|------------|----------------------------------|
| `id_end_to_end`      | varchar    | Identificador único da transação (primary key) |
| `dt_transaction`     | date       | Data de movimentação             |
| `dt_month`           | date       | Mês da movimentação              |
| `cd_seqlan`          | integer    | Código sequencial de transações  |
| `ds_transaction_type`| varchar    | Tipo da transação                |
| `vl_transaction`     | double     | Valor da transação               |

### Table: `customer`
| Column Name          | Type       | Description                      |
|----------------------|------------|----------------------------------|
| `surrogate_key`      | integer    | Identificação única do cliente (primary key) |
| `entry_date`         | date       | Data de abertura da conta        |
| `full_name`          | varchar    | Nome completo                    |
| `birth_date`         | date       | Data de nascimento               |
| `uf_name`            | varchar    | Estado de residência             |
| `uf`                 | varchar    | Sigla do estado de residência    |
| `street_name`        | varchar    | Logradouro                       |
