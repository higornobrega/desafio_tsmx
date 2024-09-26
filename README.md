# Desafio TSMX

Repositório dedicado ao desafio da TSMX

# Passo a Passo Desafio TSMX

# PostgreSQL

## Instalar BD (Banco de Dados) no Linux

```bash
sudo apt install postgresql postgresql-contrib
```

# Python

## Instalar dependências Python

```bash
sudo apt install libpq-dev python3-dev build-essencial python3-venv
```

# Projeto

## Criar ambiente virtual

Criando ambiente virtual com nome venv

```bash
python3 -m venv venv
```

Ativando venv

```bash
source venv/bin/activate
```

## Instalação das bibliotecas necessárias.

Instalando cada biblioteca pelo terminal 

```bash
pip3 install psycopg2 pandas openpyxl
```

ou você pode usar o arquivo requirements.txt onde terá todas as bibliotecas necessárias para o projeto

```bash
pip3 install -r requirements.txt
```

## Criando BD

Abra o terminal do PostgreSQL

```bash
sudo -i -u postgres
```

Entre com o cliente PostgreSQL

```bash
psql
```

Crie um banco de dados

```bash
CREATE DATABASE desafio_tsmx;
```

Crie usuário com senha

```bash
CREATE USER user_desafio_tsmx WITH PASSWORD 'senha_user_desafio_tsmx';
```

Dê permissão ao usuário

```bash
GRANT ALL PRIVILEGES ON DATABASE desafio_tsmx TO user_desafio_tsmx;
```

Saia do terminal

```bash
\q
exit
```

## **Restaurar o Schema do Banco de Dados**

```bash
sudo -i -u postgres
```

Entrando no banco de dados com usuário

```bash
psql -U user_desafio_tsmx -d desafio_tsmx
```

Use o caminho completo para restaura o schema

```bash
 \i /caminho_completo/schema_database_pgsql.sql
```
