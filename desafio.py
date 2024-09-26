import re

import pandas as pd
import psycopg2

DB_NAME = 'desafio_tsmx'
DB_USER = 'user_desafio_tsmx'
DB_PASSWORD = 'senha_user_desafio_tsmx'
DB_HOST = 'localhost'
DB_PORT = '5432'


def conectar_bd():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
        )
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None


def tratar_dados(clientes_df):

    clientes_df['Data Nasc.'] = pd.to_datetime(
        clientes_df['Data Nasc.'], errors='coerce')

    clientes_df['Data Cadastro cliente'] = pd.to_datetime(
        clientes_df['Data Cadastro cliente'], errors='coerce')

    clientes_df['Data Nasc.'] = clientes_df['Data Nasc.'].apply(
        lambda x: None if pd.isnull(x) else x)

    clientes_df['Data Cadastro cliente'] = clientes_df['Data Cadastro cliente'].apply(
        lambda x: None if pd.isnull(x) else x)

    clientes_df['Nome Fantasia'] = clientes_df['Nome Fantasia'].apply(
        lambda x: None if pd.isnull(x) else x)
    return clientes_df


def inserir_clientes(cursor, clientes_df):
    for _, row in clientes_df.iterrows():
        if str(row['Data Nasc.']) == "NaT":
            data_nascimento = None
        else:
            data_nascimento = row['Data Nasc.']

        cursor.execute("""
            SELECT id FROM tbl_clientes WHERE cpf_cnpj = %s;
        """, (row['CPF/CNPJ'],))
        cliente_id = cursor.fetchone()

        try:
            if cliente_id is None:
                cursor.execute("""
                    INSERT INTO tbl_clientes (nome_razao_social, nome_fantasia, cpf_cnpj, data_nascimento, data_cadastro)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id;
                """, (row['Nome/Razão Social'], row['Nome Fantasia'], row['CPF/CNPJ'], data_nascimento, row['Data Cadastro cliente']))

                cliente_id = cursor.fetchone()

            if cliente_id is None:
                print(
                    f"Erro: cliente_id não encontrado após inserir cliente {row['Nome/Razão Social']}")
            else:
                inserir_contatos(cursor, cliente_id[0], row)
                criar_contratos(cursor, cliente_id[0], row)
        except Exception as e:

            print(f"Erro ao inserir cliente {row['Nome/Razão Social']}: {e}")
            cursor.connection.rollback()  # Rollback da transação em caso de erro


def convert_estado_in_acronomo(estado):
    estados_brasil = {
        "Acre": "AC",
        "Alagoas": "AL",
        "Amapá": "AP",
        "Amazonas": "AM",
        "Bahia": "BA",
        "Ceará": "CE",
        "Distrito Federal": "DF",
        "Espírito Santo": "ES",
        "Goiás": "GO",
        "Maranhão": "MA",
        "Mato Grosso": "MT",
        "Mato Grosso do Sul": "MS",
        "Minas Gerais": "MG",
        "Pará": "PA",
        "Paraíba": "PB",
        "Paraná": "PR",
        "Pernambuco": "PE",
        "Piauí": "PI",
        "Rio de Janeiro": "RJ",
        "Rio Grande do Norte": "RN",
        "Rio Grande do Sul": "RS",
        "Rondônia": "RO",
        "Roraima": "RR",
        "Santa Catarina": "SC",
        "São Paulo": "SP",
        "Sergipe": "SE",
        "Tocantins": "TO"
    }
    return estados_brasil.get(estado, '')


def criar_contratos(cursor, cliente_id, row):

    verificar_plan = verificar_plano_existe(
        cursor, row['Plano'], row['Plano Valor'])

    if verificar_plan is None:
        verificar_plan = criar_planos(cursor, row['Plano'], row['Plano Valor'])
    id_tbl_status_contrato = get_tbl_status_contrato(cursor, row['Status'])
    id_tbl_status_contrato = id_tbl_status_contrato[0]
    plano_id = verificar_plan[0]
    if row['Isento'] == 'Sim':
        isento = True
    else:
        isento = False

    estado = convert_estado_in_acronomo(row['UF'])
    if estado == '':
        print("row['Nome/Razão Social'] - UF inválido")
        print(row['Nome/Razão Social'])
        return ''

    cursor.execute("""
                   INSERT INTO tbl_cliente_contratos (cliente_id, plano_id, dia_vencimento, isento, endereco_logradouro, endereco_numero, endereco_bairro, endereco_cidade, endereco_complemento, endereco_cep, endereco_uf, status_id)
                   VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   RETURNING id;
                   """, (cliente_id, plano_id, row['Vencimento'], isento, row['Endereço'], row['Número'], row['Bairro'], row['Cidade'], row['Complemento'], row['CEP'], estado, id_tbl_status_contrato))
    cursor_id = cursor.fetchone()


def get_tbl_status_contrato(cursor, status):
    cursor.execute("""
        SELECT id FROM tbl_status_contrato 
        WHERE status = %s;
    """, (status,))

    result = cursor.fetchone()
    return result


def verificar_plano_existe(cursor, plano, plano_valor):
    cursor.execute("""
        SELECT id FROM tbl_planos 
        WHERE descricao = %s AND valor = %s;
    """, (plano, plano_valor))

    result = cursor.fetchone()
    return result


def criar_planos(cursor, descricao, valor):
    cursor.execute("""
                   INSERT INTO tbl_planos (descricao, valor)
                   VALUES(%s, %s)
                   RETURNING id;
                   """, (descricao, valor))
    cursor_id = cursor.fetchone()
    return cursor_id


def inserir_contatos(cursor, cliente_id, row):

    celular = row['Celulares']
    telefone = row['Telefones']
    email = row['Emails']
    cliente_id = int(cliente_id)
    try:
        if not pd.isna(celular):
            id_tipos_contato_celular = retorna_id_tipos_contato(
                'Celular', cursor)
            criar_tbl_contatos(cursor, cliente_id,
                               id_tipos_contato_celular, celular)
        if not pd.isna(telefone):
            id_tipos_contato_telefone = retorna_id_tipos_contato(
                'Telefone', cursor)
            criar_tbl_contatos(cursor, cliente_id,
                               id_tipos_contato_telefone, telefone)

        if not pd.isna(email):
            id_tipos_contato_email = retorna_id_tipos_contato('E-Mail', cursor)
            criar_tbl_contatos(cursor, cliente_id,
                               id_tipos_contato_email, email)

    except Exception as e:
        print(
            f"Erro ao inserir contato para o cliente {cliente_id}: {e}")
        cursor.connection.rollback()


def criar_tbl_contatos(cursor, cliente_id, tipo_contato, contato):
    cursor.execute("""
                   INSERT INTO tbl_cliente_contatos (cliente_id, tipo_contato_id, contato)
                   VALUES (%s, %s, %s)
                   RETURNING id;
                   """, (cliente_id, tipo_contato, contato))


def retorna_id_tipos_contato(dado, cursor):
    cursor.execute("""
            SELECT id FROM tbl_tipos_contato WHERE tipo_contato = %s;
        """, (dado,))
    tipos_contato_id = cursor.fetchone()
    if tipos_contato_id is None:
        print(
            f"Erro: Não foi encontrado o contato do Tipo{dado}")
    return int(tipos_contato_id[0])


def validacao_dados(clientes_df):
    cpf_regex = r'^\d{3}\.\d{3}\.\d{3}-\d{2}$'
    cnpj_regex = r'^\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}$'

    invalido = []

    def verificar_linha(row):
        erros = []

        if pd.isnull(row['Nome/Razão Social']) or row['Nome/Razão Social'].strip() == '':
            erros.append('Nome/Razão Social vazio ou nulo')

        cpf_cnpj = row['CPF/CNPJ']
        if pd.isnull(cpf_cnpj) or not isinstance(cpf_cnpj, str) or not (re.match(cpf_regex, cpf_cnpj) or re.match(cnpj_regex, cpf_cnpj)):
            erros.append('CPF/CNPJ inválido')

        data_nasc = row['Data Nasc.']
        if pd.isnull(data_nasc):
            pass
        else:
            try:
                pd.to_datetime(data_nasc, errors='raise')
            except (ValueError, TypeError):  # Captura erros de tipo ou valor inválido
                erros.append('Data de Nascimento inválida')

        if erros:
            row_dict = row.to_dict()
            row_dict['erros'] = erros
            row_dict['index'] = row.name
            invalido.append(row_dict)

    clientes_df.apply(verificar_linha, axis=1)

    clientes_df_valido = clientes_df.drop([d['index'] for d in invalido])

    return clientes_df_valido, invalido


def importar_dados():
    conn = conectar_bd()
    if conn is None:
        return
    cursor = conn.cursor()

    clientes_df = pd.read_excel('dados_importacao.xlsx')

    clientes_df, invalido = validacao_dados(clientes_df)
    clientes_df = tratar_dados(clientes_df)

    try:
        inserir_clientes(cursor, clientes_df)
        cursor.execute("""
            SELECT COUNT(*) FROM tbl_clientes;
        """)

        count = cursor.fetchone()[0]

        print("X" * 40)
        print(f"TOTAL CLIENTES IMPORTADOS: {count}")
        print("X" * 40)

        for cliente in invalido:
            print("CLIENTE NÃO IMPORTADOS:")
            print(f"Nome/Razão Social: {cliente.get('Nome/Razão Social')}")
            print(f"Nome Fantasia: {cliente.get('Nome Fantasia')}")
            print(f"CPF/CNPJ: {cliente.get('CPF/CNPJ')}")
            print(f"Data Nascimento: {cliente.get('Data Nasc.')}")
            print(
                f"Data Cadastro Cliente: {cliente.get('Data Cadastro cliente')}")
            print("MOTIVO:")
            for erro in cliente.get('erros', []):
                print(f"- {erro}")
            print("-" * 40)
        conn.commit()

    except Exception as e:
        conn.rollback()


importar_dados()
