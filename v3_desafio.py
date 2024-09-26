import re

import pandas as pd
import psycopg2

# NA TABELA tbl_cliente_contato esta setado 1 no tipo_contato_id


# Configurações de conexão com o banco de dados
DB_NAME = 'desafio_tsmx'
DB_USER = 'user_desafio_tsmx'
DB_PASSWORD = 'senha_user_desafio_tsmx'
DB_HOST = 'localhost'
DB_PORT = '5432'

# Função para conectar ao banco de dados


def conectar_bd():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
        )
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None

# Função para garantir que os tipos de contato existam


def garantir_tipos_contato(cursor):
    tipos_contato = ['email', 'telefone']
    tipo_ids = {}

    for tipo in tipos_contato:
        cursor.execute(
            "SELECT id FROM tbl_tipos_contato WHERE tipo_contato = %s;", (tipo,))
        tipo_contato = cursor.fetchone()
        if tipo_contato is None:
            cursor.execute(
                "INSERT INTO tbl_tipos_contato (tipo_contato) VALUES (%s) RETURNING id;", (tipo,))
            tipo_contato_id = cursor.fetchone()[0]
            tipo_ids[tipo] = tipo_contato_id
            print(
                f"Tipo de contato '{tipo}' adicionado com id {tipo_contato_id}.")
        else:
            tipo_ids[tipo] = tipo_contato[0]
            print(
                f"Tipo de contato '{tipo}' já existe com id {tipo_contato[0]}.")

    return tipo_ids


def tratar_dados(clientes_df):
    # Corrigir valores 'NaT' no campo de data e substituí-los por None
    clientes_df['Data Nasc.'] = pd.to_datetime(
        clientes_df['Data Nasc.'], errors='coerce')
    clientes_df['Data Cadastro cliente'] = pd.to_datetime(
        clientes_df['Data Cadastro cliente'], errors='coerce')

    # Substituir NaT por None para compatibilidade com o banco de dados
    clientes_df['Data Nasc.'] = clientes_df['Data Nasc.'].apply(
        lambda x: None if pd.isnull(x) else x)
    clientes_df['Data Cadastro cliente'] = clientes_df['Data Cadastro cliente'].apply(
        lambda x: None if pd.isnull(x) else x)

    clientes_df['Nome Fantasia'] = clientes_df['Nome Fantasia'].apply(
        lambda x: None if pd.isnull(x) else x)
    return clientes_df

# Função para inserir os dados na tabela 'tbl_clientes'


def validar_cpf_cnpj(cpf_cnpj, nome_razao_social):
    # Converter para string caso o valor não seja nulo
    cpf_cnpj_padrao = cpf_cnpj
    cpf_cnpj = str(cpf_cnpj)

    # Expressão regular para CPF no formato xxx.xxx.xxx-xx
    cpf_pattern = r'^\d{3}\.\d{3}\.\d{3}-\d{2}$'

    # Expressão regular para CNPJ no formato xx.xxx.xxx/xxxx-xx
    cnpj_pattern = r'^\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}$'

    # Verifica se o valor corresponde ao padrão de CPF ou CNPJ
    if re.match(cpf_pattern, cpf_cnpj) or re.match(cnpj_pattern, cpf_cnpj):
        return True  # O valor está no padrão correto
    else:
        print(
            f"O CPF/CNPJ {cpf_cnpj_padrao} com nome {nome_razao_social} está fora do padrão exigido.")
        return False  # O valor está fora do padrão


def inserir_clientes(cursor, clientes_df):
    for _, row in clientes_df.iterrows():
        if str(row['Data Nasc.']) == "NaT":
            data_nascimento = None
        else:
            data_nascimento = row['Data Nasc.']

        valid_cpf = validar_cpf_cnpj(row['CPF/CNPJ'], row['Nome/Razão Social'])
        if not valid_cpf:
            continue

        cursor.execute("""
            SELECT id FROM tbl_clientes WHERE cpf_cnpj = %s;
        """, (row['CPF/CNPJ'],))
        exists = cursor.fetchone()

        if exists is not None:
            print(
                f"Erro: CPF/CNPJ {row['CPF/CNPJ']} JÁ CADASTRADO")
            continue

        try:
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
# Função para inserir os contatos dos clientes


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
    # Agora que temos certeza que `verificar_plan` não é None, podemos printar o id
    # O retorno é uma tupla, pegamos o primeiro elemento
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
    return result  # Retorna a tupla com o id do plano (ex: (4800,))


def verificar_plano_existe(cursor, plano, plano_valor):
    cursor.execute("""
        SELECT id FROM tbl_planos 
        WHERE descricao = %s AND valor = %s;
    """, (plano, plano_valor))

    result = cursor.fetchone()
    return result  # Retorna a tupla com o id do plano (ex: (4800,))


def criar_planos(cursor, descricao, valor):
    cursor.execute("""
                   INSERT INTO tbl_planos (descricao, valor)
                   VALUES(%s, %s)
                   RETURNING id;
                   """, (descricao, valor))
    cursor_id = cursor.fetchone()  # Retorna a tupla com o id do novo plano
    return cursor_id  # Retorna a tupla para consistência com a função `verificar_plano_existe`


def inserir_contatos(cursor, cliente_id, row):

    # Verificar se há valores 'nan' e tratá-los
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
        cursor.connection.rollback()  # Rollback da transação em caso de erro

# Função para inserir os contratos dos clientes


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


# def inserir_contratos(cursor, contratos_df):
#     for _, row in contratos_df.iterrows():
#         try:

#             cursor.execute("""
#                 SELECT id FROM tbl_clientes WHERE cpf_cnpj = %s;
#             """, (row['CPF/CNPJ'],))
#             cliente_id = cursor.fetchone()

#             if cliente_id is None:
#                 print(
#                     f"Erro: cliente_id não encontrado para o CPF/CNPJ {row['CPF/CNPJ']}")
#                 continue

#             cursor.execute("""
#                 SELECT id FROM tbl_planos WHERE descricao = %s;
#             """, (row['Plano'],))
#             plano_id = cursor.fetchone()

#             if plano_id is None:
#                 print(
#                     f"Erro: plano_id não encontrado para o plano {row['Plano']}")
#                 plano_id = [1]
#                 # continue

#             cursor.execute("""
#                 SELECT id FROM tbl_status_contrato WHERE status = %s;
#             """, (row['Status'],))
#             status_id = cursor.fetchone()

#             if status_id is None:
#                 print(
#                     f"Erro: status_id não encontrado para o status {row['Status']}")
#                 status_id = [1]
#                 # continue

#             cursor.execute("""
#                 INSERT INTO tbl_cliente_contratos (
#                     cliente_id, plano_id, dia_vencimento, isento, endereco_logradouro, endereco_numero,
#                     endereco_bairro, endereco_cidade, endereco_complemento, endereco_cep, endereco_uf, status_id
#                 )
#                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
#             """, (
#                 cliente_id[0], plano_id[0], row['Vencimento'], row['Isento'], row['Endereço'],
#                 row['Número'], row['Bairro'], row['Cidade'], row['Complemento'],
#                 row['CEP'], row['UF'], status_id[0]
#             ))
#         except Exception as e:
#             print(
#                 f"Erro ao inserir contrato para cliente {row['cpf_cnpj']}: {e}")
#             cursor.connection.rollback()  # Rollback da transação em caso de erro

# Função principal para a importação dos dados

# NA TABELA tbl_cliente_contato esta setado 1 no tipo_contato_id

def validacao_dados(clientes_df):
    # Definindo os padrões para CPF e CNPJ
    cpf_regex = r'^\d{3}\.\d{3}\.\d{3}-\d{2}$'
    cnpj_regex = r'^\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}$'

    invalido = []

    # Função que marca os erros no dataset em uma única iteração
    def verificar_linha(row):
        erros = []

        # Verificar Nome/Razão Social vazio ou nulo
        if pd.isnull(row['Nome/Razão Social']) or row['Nome/Razão Social'].strip() == '':
            erros.append('Nome/Razão Social vazio ou nulo')

        # Verificar se o CPF/CNPJ tem formato inválido
        cpf_cnpj = row['CPF/CNPJ']
        if pd.isnull(cpf_cnpj) or not isinstance(cpf_cnpj, str) or not (re.match(cpf_regex, cpf_cnpj) or re.match(cnpj_regex, cpf_cnpj)):
            erros.append('CPF/CNPJ inválido')

        if erros:
            # Adicionar a linha e os erros encontrados à lista de inválidos
            row_dict = row.to_dict()
            row_dict['erros'] = erros
            row_dict['index'] = row.name  # Adicionar o índice manualmente
            invalido.append(row_dict)

    # Aplicar a verificação em todas as linhas
    clientes_df.apply(verificar_linha, axis=1)

    # Remover as linhas inválidas do DataFrame
    clientes_df_valido = clientes_df.drop([d['index'] for d in invalido])

    return clientes_df_valido, invalido


def importar_dados():
    # Conectar ao banco de dados
    conn = conectar_bd()
    if conn is None:
        return
    cursor = conn.cursor()

    # Garantir que os tipos de contato existem

    # Ler os dados do Excel
    clientes_df = pd.read_excel('dados_importacao_editado_1.xlsx')

    # Tratar dados antes da inserção
    clientes_df = tratar_dados(clientes_df)

    # Remover as linhas inválidas do DataFrame
    clientes_df, invalido = validacao_dados(clientes_df)

    # Exibir os dados inválidos e o DataFrame atualizado
    print(
        f'Cliente {invalido} não foi inserido pois não possui Nome/Razão Social')

    try:
        # NA TABELA tbl_cliente_contato esta setado 1 no tipo_contato_id
        inserir_clientes(cursor, clientes_df)
        # # Commitar as transações
        conn.commit()

    except Exception as e:
        conn.rollback()  # Em caso de erro


importar_dados()
