# -*- coding: utf-8 -*-
import os
import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
from flask_caching import Cache
from flask_compress import Compress
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from itertools import groupby
from operator import itemgetter
from babel.dates import format_date


# E adicione format_date a ela, assim:
from babel.dates import format_date

app = Flask(__name__)
app.config['CACHE_TYPE'] = 'SimpleCache'
cache = Cache()
compress = Compress()
limiter = Limiter(
    key_func=get_remote_address,
    default_limits=["200 per day", "50 per hour"]
)
cache.init_app(app)
compress.init_app(app)
limiter.init_app(app)

# Security headers
@app.after_request
def add_security_headers(response):
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
    return response
from werkzeug.security import generate_password_hash, check_password_hash
from dotenv import load_dotenv
import logging
import re
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation # Adicionado ROUND_HALF_UP, InvalidOperation
import json
from datetime import date, timedelta, datetime
from math import ceil
from dateutil.relativedelta import relativedelta
from dateutil.rrule import rrule, MONTHLY, YEARLY, DAILY 
from babel.numbers import format_currency # Para calcular data de conclusão da meta e recorrências
# E adicione format_date a ela, assim:


# Carrega variáveis de ambiente
load_dotenv()

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'fallback_inseguro_trocar_em_producao')

# --- Conexão com DB ---
def get_db_connection():
    conn = None
    try:
        conn = psycopg2.connect(
            host=os.environ.get('DB_HOST', 'localhost'),
            database=os.environ.get('DB_NAME', 'postgres'),
            user=os.environ.get('DB_USER', 'postgres'),
            password=os.environ.get('DB_PASSWORD', 'typebot')
        )
        return conn
    except psycopg2.Error as e:
        logging.error(f"Erro ao conectar ao PostgreSQL: {e}")
        if conn: conn.close()
        return None


# --- Funções Auxiliares ---
def gerar_hash_senha(senha):
    return generate_password_hash(senha, method='pbkdf2:sha256')

def verificar_senha(hash_armazenado, senha_fornecida):
    if not hash_armazenado: return False
    return check_password_hash(hash_armazenado, senha_fornecida)

def gerar_nome_schema(telefone_whatsapp):
    if not telefone_whatsapp: return None
    numeros_telefone = re.sub(r'\D', '', telefone_whatsapp)
    if not numeros_telefone: return None
    return f"user{numeros_telefone}"

# Filtro CORRIGIDO para formatar datas com localidade (espanhol mexicano)
def format_date_locale(value, format_string=None, locale='es_MX'):
    if not isinstance(value, (date, datetime)):
        return value
    # Usa a string de formato passada ou 'full' como padrão
    fmt = format_string if format_string is not None else 'full'
    return format_date(value, format=fmt, locale=locale)

# A linha de registro do filtro continua a mesma
app.jinja_env.filters['localedate'] = format_date_locale

def buscar_categorias_por_tipo(conn, user_schema, tipo_categoria):
    """
    Busca todas as categorias disponíveis para um determinado tipo.
    Retorna uma lista de nomes de categorias ordenadas.
    """
    if not conn or not user_schema or not tipo_categoria:
        return []

    cur = None
    try:
        cur = conn.cursor()
        query = sql.SQL("""
            SELECT nome 
            FROM {schema}.categorias 
            WHERE tipo = %s 
            ORDER BY is_fixa DESC, nome ASC
        """).format(schema=sql.Identifier(user_schema))

        cur.execute(query, (tipo_categoria,))
        return [row[0] for row in cur.fetchall()]
    except psycopg2.Error as e:
        logging.error(f"Erro ao buscar categorias do tipo {tipo_categoria}: {e}")
        return []
    finally:
        if cur: cur.close()


def buscar_metodos_pagamento_ativos(conn, user_schema):
    """
    Busca todos os métodos de pagamento ativos.
    Retorna uma lista de dicionários com id, nome e tipo.
    """
    if not conn or not user_schema:
        return []

    cur = None
    try:
        cur = conn.cursor(cursor_factory=DictCursor)
        query = sql.SQL("""
            SELECT id, nome, tipo, modalidad
            FROM {schema}.metodos_pagamento 
            WHERE ativo = TRUE 
            ORDER BY nome ASC
        """).format(schema=sql.Identifier(user_schema))

        cur.execute(query)
        return cur.fetchall()
    except psycopg2.Error as e:
        logging.error(f"Erro ao buscar métodos de pagamento ativos: {e}")
        return []
    finally:
        if cur: cur.close()



# -*- coding: utf-8 -*-
# ... (outras importações no topo do seu app.py)
# Certifique-se de que estas importações de dateutil estão presentes:
from dateutil.rrule import rrule, MONTHLY, YEARLY, DAILY # Você já deve ter WEEKLY se descomentar o código
# from dateutil.rrule import WEEKLY # Descomente se for usar 'semanal'

# ... (resto do seu código Flask: app = Flask(...), get_db_connection(), etc.)

# Função auxiliar para mapear recorrência (VERSÃO ATUALIZADA)
def get_rrule_params(recurrencia_str_original):
    if not isinstance(recurrencia_str_original, str):  # Verificação de tipo
        logging.warning(f"get_rrule_params: Tipo de entrada inválido '{type(recurrencia_str_original)}', esperado string.")
        return None

    # Dicionário para traduzir variações comuns para termos padronizados
    traducoes = {
        'mensal': 'mensual',     # Português -> Espanhol (compatibilidade)
        'monthly': 'mensual',    # Inglês -> Espanhol
        'anualmente': 'anual',   # Variação -> Padrão
        'yearly': 'anual',       # Inglês -> Espanhol
        'unico': 'unico',        # Mantém 'unico' (já que 'único' com acento pode variar)
        'único': 'unico',        # Português com acento -> Padrão sem acento
        'única': 'unico',        # Variação feminina -> Padrão sem acento
        'mensual': 'mensual',    # Mantém mensual como mensual
        # Adicione mais traduções conforme necessário. Ex:
        # 'weekly': 'semanal',
        # 'daily': 'diario',
    }

    # 1. Limpa a string: minúsculas e remove espaços extras nas bordas
    recurrencia_limpa = recurrencia_str_original.lower().strip()

    # 2. Aplica a tradução para um termo padronizado
    #    Se o termo limpo não estiver no dicionário de traduções, usa o próprio termo limpo.
    termo_padronizado = traducoes.get(recurrencia_limpa, recurrencia_limpa)

    # 3. Mapeia o termo padronizado para os parâmetros do rrule
    if termo_padronizado == 'mensual':
        return {'freq': MONTHLY, 'interval': 1}
    elif termo_padronizado == 'bimestral':
        return {'freq': MONTHLY, 'interval': 2}
    elif termo_padronizado == 'trimestral':
        return {'freq': MONTHLY, 'interval': 3}
    elif termo_padronizado == 'semestral':
        return {'freq': MONTHLY, 'interval': 6}
    elif termo_padronizado == 'anual':
        return {'freq': YEARLY, 'interval': 1}
    # elif termo_padronizado == 'semanal':  # Exemplo se adicionar 'semanal'
    #     return {'freq': WEEKLY, 'interval': 1}
    # elif termo_padronizado == 'diario':   # Exemplo se adicionar 'diario'
    #     return {'freq': DAILY, 'interval': 1}
    elif termo_padronizado == 'unico':
        # Para 'unico', a função retorna None, e a lógica principal do relatório tratará disso.
        return None
    else:
        # Se, após limpeza e tradução, o termo ainda não for reconhecido.
        logging.warning(f"Recorrência desconhecida. Original: '{recurrencia_str_original}', Processada como: '{termo_padronizado}'. Nenhuma regra definida corresponde.")
        return None


def format_currency_filter(value):
    if value is None:
        # Para valores nulos, você pode retornar o formato MXN desejado
        return format_currency(0, 'MXN', locale='es_MX') # Ex: "$0.00" ou "$0.00 MXN"
    try:
        # Converte para Decimal para precisão, se necessário, mas format_currency aceita float/int
        val_decimal = Decimal(value)
        # Formata usando Babel para o locale es_MX e moeda MXN
        return format_currency(val_decimal, 'MXN', locale='es_MX')
    except (InvalidOperation, TypeError, ValueError):
        # Fallback para erro, também no formato MXN
        # Você pode decidir o que mostrar em caso de erro, ex: "$ -" ou um valor específico
        return "$ -" # Simples fallback
app.jinja_env.filters['currency'] = format_currency_filter

def garantir_colunas_metodo_pagamento(conn, user_schema):
    """
    Garante que as colunas metodo_pagamento_id e campos relacionados existam nas tabelas.
    """
    if not conn or not user_schema:
        return False
    
    cur = None
    try:
        cur = conn.cursor()
        
        # Verificar e criar colunas na tabela gastos
        try:
            # Coluna de referência
            alter_query_gastos_id = sql.SQL("""
                ALTER TABLE {schema}.gastos 
                ADD COLUMN IF NOT EXISTS metodo_pagamento_id INTEGER 
                REFERENCES {schema}.metodos_pagamento(id)
            """).format(schema=sql.Identifier(user_schema))
            cur.execute(alter_query_gastos_id)
            
            # Colunas desnormalizadas (para compatibilidade com o novo SQL)
            alter_query_gastos_nome = sql.SQL("""
                ALTER TABLE {schema}.gastos 
                ADD COLUMN IF NOT EXISTS metodo_nome VARCHAR(100)
            """).format(schema=sql.Identifier(user_schema))
            cur.execute(alter_query_gastos_nome)
            
            alter_query_gastos_tipo = sql.SQL("""
                ALTER TABLE {schema}.gastos 
                ADD COLUMN IF NOT EXISTS metodo_tipo VARCHAR(30)
            """).format(schema=sql.Identifier(user_schema))
            cur.execute(alter_query_gastos_tipo)
            
            alter_query_gastos_modalidad = sql.SQL("""
                ALTER TABLE {schema}.gastos 
                ADD COLUMN IF NOT EXISTS metodo_modalidad VARCHAR(20)
            """).format(schema=sql.Identifier(user_schema))
            cur.execute(alter_query_gastos_modalidad)
            
        except psycopg2.Error:
            pass  # Colunas já existem ou erro não crítico
        
        # Verificar e criar coluna na tabela gastos_fixos
        try:
            alter_query_fixos = sql.SQL("""
                ALTER TABLE {schema}.gastos_fixos 
                ADD COLUMN IF NOT EXISTS metodo_pagamento_id INTEGER 
                REFERENCES {schema}.metodos_pagamento(id)
            """).format(schema=sql.Identifier(user_schema))
            cur.execute(alter_query_fixos)
        except psycopg2.Error:
            pass  # Coluna já existe ou erro não crítico
        
        conn.commit()
        return True
        
    except Exception as e:
        logging.error(f"Erro ao garantir colunas método pagamento para schema {user_schema}: {e}")
        return False
    finally:
        if cur: cur.close()


def validar_categoria(conn, user_schema, nome_categoria, tipo_esperado):
    """
    Verifica se uma categoria existe na tabela de categorias para o schema e tipo especificados.
    Retorna True se a categoria for válida, False caso contrário.
    """
    if not conn or not user_schema or not nome_categoria or not tipo_esperado:
        logging.warning("validar_categoria: Parâmetros inválidos recebidos.")
        return False

    cur = None
    try:
        cur = conn.cursor()
        query = sql.SQL("""
            SELECT 1
            FROM {schema}.categorias
            WHERE nome = %s AND tipo = %s
            LIMIT 1
        """).format(schema=sql.Identifier(user_schema))
        cur.execute(query, (nome_categoria, tipo_esperado))
        return cur.fetchone() is not None # Retorna True se encontrou (fetchone não é None)
    except psycopg2.Error as e:
        logging.error(f"Erro DB ao validar categoria '{nome_categoria}' ({tipo_esperado}) no schema {user_schema}: {e}")
        return False # Assume inválida em caso de erro
    finally:
        if cur: cur.close()


def format_date_filter(value, format_str='%d/%m/%Y'):
    if value is None:
        return "N/A"
    if isinstance(value, str):
        # Tenta converter string no formato YYYY-MM-DD para data formatada
        try:
            date_obj = datetime.strptime(value, '%Y-%m-%d').date()
            return date_obj.strftime(format_str)
        except ValueError:
            logging.warning(f"format_date_filter: String de data inválida '{value}'")
            return "N/A"
    if not isinstance(value, (date, datetime)):
        logging.warning(f"format_date_filter: Tipo inválido '{type(value)}' para valor '{value}'")
        return "N/A"
    try: 
        return value.strftime(format_str)
    except ValueError as e:
        logging.error(f"format_date_filter: Erro ao formatar data '{value}': {e}")
        return str(value)
app.jinja_env.filters['date'] = format_date_filter

# --- Função para converter tipos não serializáveis em JSON ---
def json_converter(obj):
    if isinstance(obj, Decimal): return float(obj)
    if isinstance(obj, (date, datetime)): return obj.isoformat()
    if isinstance(obj, bool): return obj
    try: return str(obj)
    except Exception: return None





# --- Rotas ---
@app.route('/')
def index():
    if 'user_assinatura_id' in session: return redirect(url_for('dashboard'))
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if 'user_assinatura_id' in session:
        return redirect(url_for('dashboard'))
    if request.method == 'POST':
        email = request.form.get('email')
        senha = request.form.get('senha')
        if not email or not senha:
            flash('El correo electrónico y la contraseña son obligatorios.', 'danger')
            return redirect(url_for('login'))
        conn = get_db_connection()
        if conn:
            cur = None
            try:
                cur = conn.cursor(cursor_factory=DictCursor)
                cur.execute("SELECT id, email, senha_hash, id_cliente_assinatura FROM clientes.dashboard_usuarios WHERE email = %s", (email,))
                login_user = cur.fetchone()
                if login_user and verificar_senha(login_user['senha_hash'], senha):
                    cur.execute("SELECT id_interno, telefone_whatsapp, nome_cliente FROM clientes.assinaturas WHERE id_interno = %s", (login_user['id_cliente_assinatura'],))
                    assinatura_info = cur.fetchone()
                    if assinatura_info:
                        schema_name = gerar_nome_schema(assinatura_info['telefone_whatsapp'])
                        if schema_name:
                            session.clear()
                            session['user_dashboard_id'] = login_user['id']
                            session['user_assinatura_id'] = assinatura_info['id_interno']
                            session['user_email'] = login_user['email']
                            session['user_schema'] = schema_name
                            session['user_nome'] = assinatura_info['nome_cliente']
                            session.permanent = True
                            session.modified = True
                            logging.info(f"Login bem-sucedido: {login_user['email']}, Schema: {schema_name}")
                            return redirect(url_for('dashboard'))
                        else:
                            logging.error(f"Não foi possível gerar nome do schema para usuário {email}.")
                            flash('Erro interno ao determinar o schema do usuário.', 'danger')
                    else:
                        logging.error(f"Assinatura ID {login_user['id_cliente_assinatura']} não encontrada para usuário {email}.")
                        flash('Erro interno: dados da assinatura não encontrados.', 'danger')
                else:
                    logging.warning(f"Tentativa de login falhou para: {email} (email não cadastrado ou senha incorreta)")
                    flash('Correo electrónico o contraseña incorrectos.', 'danger')
            except psycopg2.Error as e:
                logging.error(f"Erro de banco de dados durante o login para {email}: {e}")
                flash('Error en la base de datos durante el inicio de sesión.', 'danger')
            finally:
                if cur: cur.close()
                if conn: conn.close()
        else:
            flash('No fue posible conectarse a la base de datos.', 'danger')
        # Se chegou aqui, algo deu errado, renderiza login novamente
        return render_template('login.html') # Renderiza fora do if/else de conexão
    # Se for GET request
    return render_template('login.html')




@app.route('/criar-conta', methods=['GET', 'POST'])
def criar_conta():
    """
    Rota para exibir o formulário de criação de conta do dashboard
    e processar a submissão, validando contra a tabela de assinaturas.
    """
    if request.method == 'POST':
        email = request.form.get('email')
        senha = request.form.get('senha')
        senha_confirmacao = request.form.get('senha_confirmacao')

        # --- Validações Iniciais ---
        if not email or not senha or not senha_confirmacao:
            flash('Por favor, completa tu correo electrónico, contraseña y confirmación.', 'danger')
            # Renderiza o template de novo, passando o email digitado de volta (opcional)
            return render_template('criar_conta.html', email_previo=email)

        if senha != senha_confirmacao:
            flash('Las contraseñas ingresadas no coinciden.', 'danger')
            return render_template('criar_conta.html', email_previo=email)

        # Você pode adicionar mais validações aqui (formato do email, força da senha, etc.)

        conn = get_db_connection()
        if not conn:
            flash('Error crítico: No fue posible conectar con la base de datos.', 'danger')
            # Em caso de erro grave, talvez redirecionar para login seja melhor
            return redirect(url_for('login'))

        cur = None
        try:
            cur = conn.cursor(cursor_factory=DictCursor)

            # 1. Verificar se o email JÁ TEM um acesso ao dashboard
            cur.execute("SELECT id FROM clientes.dashboard_usuarios WHERE email = %s", (email,))
            if cur.fetchone():
                flash('Este correo electrónico ya tiene acceso al dashboard. Intenta iniciar sesión o recuperar tu contraseña.', 'warning')
                return redirect(url_for('login')) # Leva para login se já existe

            # 2. Verificar se o email EXISTE na tabela de assinaturas
            cur.execute("SELECT id_interno FROM clientes.assinaturas WHERE email = %s LIMIT 1", (email,))
            assinatura = cur.fetchone()

            if not assinatura:
                flash('Correo electrónico no encontrado en nuestras suscripciones activas. Verifica que lo hayas escrito correctamente o ponte en contacto con soporte.', 'danger')
                return render_template('criar_conta.html', email_previo=email) # Mostra form de novo

            # Se chegou aqui, o email existe na tabela de assinaturas e não tem acesso ao dashboard ainda
            id_cliente_assinatura_encontrado = assinatura['id_interno']

            # 3. Gerar hash da senha e Inserir no banco de dados
            senha_hashed = gerar_hash_senha(senha) # Usa a função que você já tem

            insert_query = sql.SQL("""
                INSERT INTO clientes.dashboard_usuarios (email, senha_hash, id_cliente_assinatura)
                VALUES (%s, %s, %s)
            """)
            cur.execute(insert_query, (email, senha_hashed, id_cliente_assinatura_encontrado))
            conn.commit()

            flash('¡Acceso al dashboard creado con éxito! Ya puedes iniciar sesión.', 'success')
            logging.info(f"Novo acesso dashboard criado para email: {email}, ID Assinatura: {id_cliente_assinatura_encontrado}")
            return redirect(url_for('login')) # Redireciona para login após criar com sucesso

        except psycopg2.Error as e:
            conn.rollback() # Desfaz a transação em caso de erro
            flash('Error en la base de datos al intentar crear el acceso. Intenta nuevamente o contacta a soporte.', 'danger')
            logging.error(f"Erro DB (criar_conta) para {email}: {e}")
            # Retorna para o formulário em caso de erro de banco
            return render_template('criar_conta.html', email_previo=email)
        except Exception as e:
            conn.rollback()
            flash('Ocurrió un error inesperado. Por favor, inténtalo de nuevo más tarde.', 'danger')
            logging.error(f"Erro inesperado (criar_conta) para {email}: {e}", exc_info=True)
            return render_template('criar_conta.html', email_previo=email)
        finally:
            # Garante que cursor e conexão sejam fechados
            if cur: cur.close()
            if conn: conn.close()

    else: # Método GET
        # Apenas mostra o formulário de criação
        # Certifique-se que o arquivo 'templates/criar_conta.html' existe
        return render_template('criar_conta.html')


# ... (Resto das suas rotas: login, logout, dashboard, etc.)

# --- Rota para solicitar reset de senha ---
@app.route('/esqueci-senha', methods=['GET'])
def esqueci_senha_request():
    """Página com instruções para resetar senha via Moneda.ai"""
    return render_template('esqueci_senha.html')


@app.route('/receitas/<int:item_id>/edit', methods=['POST'])
def edit_outra_receita(item_id):
    """
    Processa a submissão do formulário de edição de uma 'Outra Receita'.
    """
    if 'user_assinatura_id' not in session:
        flash('Sessão expirada. Por favor, faça login novamente.', 'warning')
        return redirect(url_for('login'))

    user_schema = session.get('user_schema')
    redirect_url = url_for('receitas') # Redireciona de volta para a lista de receitas

    if not user_schema:
        flash('Erro interno: Schema do usuário não encontrado.', 'danger')
        session.clear()
        return redirect(url_for('login'))

    logging.info(f"Tentativa de editar Outra Receita ID {item_id} no schema {user_schema}")

    # 1. Obter dados do formulário
    #    Os nomes dos campos devem corresponder aos atributos 'name' no form do modal de edição
    descricao_form = request.form.get('descricao_outra_receita')
    valor_str = request.form.get('valor_outra_receita')
    categoria_form = request.form.get('categoria_outra_receita')
    data_str = request.form.get('data_outra_receita') # Campo 'data_outra_receita'

    # 2. Validação dos campos obrigatórios
    required_fields_check = [descricao_form, valor_str, categoria_form, data_str]
    required_field_names = ['descrição', 'valor', 'categoria', 'data']
    if not all(field is not None and field.strip() != '' for field in required_fields_check):
        missing = [name for name, field in zip(required_field_names, required_fields_check) if not field or field.strip() == '']
        flash(f'Erro: Preencha todos os campos obrigatórios ({", ".join(missing)}).', 'danger')
        logging.warning(f"Edição de Outra Receita falhou para ID {item_id}: campos obrigatórios ausentes: {missing}.")
        return redirect(redirect_url) # Redireciona se faltar campo

    # 3. Validação e conversão do Valor
    try:
        valor_decimal = Decimal(valor_str.replace(',', '.'))
        if valor_decimal <= 0:
            raise ValueError("O valor da receita deve ser um número positivo.")
    except (InvalidOperation, ValueError) as e:
        flash(f'Erro: Valor monetário inválido ({valor_str}). {e}', 'danger')
        logging.warning(f"Edição de Outra Receita falhou para ID {item_id}: valor inválido '{valor_str}'.")
        return redirect(redirect_url)

    # 4. Validação e conversão da Data
    try:
        data_obj = datetime.strptime(data_str, '%Y-%m-%d').date()
    except ValueError:
        flash('Erro: Formato de data inválido. Use AAAA-MM-DD.', 'danger')
        logging.warning(f"Edição de Outra Receita falhou para ID {item_id}: data inválida '{data_str}'.")
        return redirect(redirect_url)

    # 5. Conexão com o Banco de Dados
    conn = get_db_connection()
    if not conn:
        flash('Erro: Falha ao conectar com o banco de dados.', 'danger')
        return redirect(redirect_url)

    # 6. Validação da Categoria usando a função validar_categoria
    if not validar_categoria(conn, user_schema, categoria_form, 'receita'):
        flash(f'Erro: Categoria "{categoria_form}" inválida ou não permitida para Receita.', 'danger')
        logging.warning(f"Edição de Outra Receita falhou para ID {item_id}: categoria inválida '{categoria_form}'.")
        conn.close()
        return redirect(redirect_url)

    # 7. Execução do UPDATE
    cur = None
    try:
        cur = conn.cursor()
        # Monta a query UPDATE
        update_query = sql.SQL("""
            UPDATE {schema}.outras_receitas
            SET descripcion = %s, valor = %s, categoria = %s, fecha = %s
            WHERE id = %s
        """).format(schema=sql.Identifier(user_schema))

        query_params = (descricao_form, valor_decimal, categoria_form, data_obj, item_id)

        logging.debug(f"Executando UPDATE Outra Receita: Query={update_query.as_string(conn)} Params={query_params}")
        cur.execute(update_query, query_params)
        conn.commit()

        if cur.rowcount > 0:
            flash('¡Ingreso actualizado con éxito!', 'success')
            logging.info(f"Outra Receita ID {item_id} atualizada com sucesso no schema {user_schema}.")
        else:
            flash('Ingreso no encontrado o ningún dato fue modificado.', 'warning')
            logging.warning(f"Edição para Outra Receita ID {item_id}: rowcount foi 0.")

    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash(f'Error: {e}', 'danger')
        logging.error(f"Erro DB (psycopg2) ao editar Outra Receita ID {item_id} schema {user_schema}: {e}")
    except Exception as e:
        if conn: conn.rollback()
        flash(f'Error.', 'danger')
        logging.error(f"Erro inesperado (Python) ao editar Outra Receita ID {item_id} schema {user_schema}: {e}", exc_info=True)
    finally:
        if cur: cur.close()
        if conn: conn.close()

    return redirect(redirect_url)



@app.route('/categorias')
def categorias():
    """Exibe a página de gerenciamento de categorias com dados de gastos mensais."""
    if 'user_assinatura_id' not in session:
        flash('Você precisa fazer login para acessar esta página.', 'warning')
        return redirect(url_for('login'))

    user_schema = session.get('user_schema')
    user_nome = session.get('user_nome', session.get('user_email'))
    if not user_schema:
        flash('Erro interno: Informações do usuário incompletas.', 'danger')
        session.clear(); return redirect(url_for('login'))

    conn = get_db_connection()
    if not conn:
        flash('Erro de conexão com o banco de dados.', 'danger')
        return render_template('categorias.html', categorias_por_tipo={})

    # Dicionário para armazenar o gasto total do mês por categoria
    gastos_do_mes = {}
    lista_categorias_enriquecida = []

    cur = None
    try:
        cur = conn.cursor(cursor_factory=DictCursor)
        
        # 1. Obter o primeiro e o último dia do mês atual
        hoje = date.today()
        primeiro_dia_mes = hoje.replace(day=1)
        # Calcula o último dia do mês de forma segura
        ultimo_dia_mes = (primeiro_dia_mes + relativedelta(months=1)) - timedelta(days=1)
        
        logging.info(f"Calculando gastos para o período: {primeiro_dia_mes} a {ultimo_dia_mes}")

        # 2. Calcular gastos variáveis do mês e agrupar por categoria
        query_gastos_var = sql.SQL("""
            SELECT categoria, SUM(valor) as total
            FROM {schema}.gastos
            WHERE data BETWEEN %s AND %s
            GROUP BY categoria
        """).format(schema=sql.Identifier(user_schema))
        cur.execute(query_gastos_var, (primeiro_dia_mes, ultimo_dia_mes))
        for row in cur.fetchall():
            gastos_do_mes[row['categoria']] = row['total']

        # 3. Calcular gastos fixos do mês e somar aos totais
        query_gastos_fixos = sql.SQL("""
            SELECT categoria, valor, fecha_inicio, recurrencia
            FROM {schema}.gastos_fixos
            WHERE activo = TRUE AND fecha_inicio <= %s
        """).format(schema=sql.Identifier(user_schema))
        cur.execute(query_gastos_fixos, (ultimo_dia_mes,))
        for gf in cur.fetchall():
            rrule_params = get_rrule_params(gf['recurrencia'])
            if rrule_params:
                occurrences = rrule(dtstart=gf['fecha_inicio'], until=ultimo_dia_mes, **rrule_params)
                for occ in occurrences:
                    if occ.date() >= primeiro_dia_mes:
                        categoria_nome = gf['categoria']
                        gastos_do_mes[categoria_nome] = gastos_do_mes.get(categoria_nome, Decimal('0.00')) + gf['valor']
        
        # 4. Buscar todas as categorias
        query_categorias = sql.SQL("""
            SELECT id, nome, tipo, is_fixa, limite
            FROM {schema}.categorias
            ORDER BY tipo, is_fixa DESC, nome ASC
        """).format(schema=sql.Identifier(user_schema))
        cur.execute(query_categorias)
        lista_categorias_raw = cur.fetchall()

        # 5. Enriquecer cada categoria com o seu gasto atual
        for cat in lista_categorias_raw:
            cat_dict = dict(cat)
            gasto_atual = gastos_do_mes.get(cat_dict['nome'], Decimal('0.00'))
            cat_dict['gasto_atual'] = gasto_atual
            lista_categorias_enriquecida.append(cat_dict)
            
        logging.info(f"Categorias buscadas e enriquecidas para schema {user_schema}: {len(lista_categorias_enriquecida)} encontradas.")

    except psycopg2.Error as e:
        flash('Erro ao buscar dados no banco de dados.', 'danger')
        logging.error(f"Erro DB ao buscar/processar categorias (schema {user_schema}): {e}")
    except Exception as e:
        flash('Ocorreu um erro inesperado ao processar os dados.', 'danger')
        logging.error(f"Erro inesperado ao processar categorias (schema {user_schema}): {e}", exc_info=True)
    finally:
        if cur: cur.close()
        if conn: conn.close()

    # 6. Organizar as categorias enriquecidas por tipo para o template
    categorias_por_tipo = {
        'receita': [cat for cat in lista_categorias_enriquecida if cat['tipo'] == 'receita'],
        'gasto_variavel': [cat for cat in lista_categorias_enriquecida if cat['tipo'] == 'gasto_variavel'],
        'gasto_fixo': [cat for cat in lista_categorias_enriquecida if cat['tipo'] == 'gasto_fixo'],
    }

    return render_template('categorias.html',
                           user_nome=user_nome,
                           categorias_por_tipo=categorias_por_tipo)


# --- Rota para ADICIONAR Categoria (Atualizada SEM COR e com Limite) ---
@app.route('/categorias/add', methods=['POST'])
def add_categoria():
    """Processa o formulário de adição de nova categoria, com limite para gastos variáveis."""
    if 'user_assinatura_id' not in session:
        flash('Sessão expirada.', 'warning'); return redirect(url_for('login'))

    user_schema = session.get('user_schema')
    redirect_url = url_for('categorias')
    if not user_schema:
        flash('Erro interno.', 'danger'); session.clear(); return redirect(url_for('login'))

    # Corrigindo os nomes dos campos para corresponder ao HTML
    nome_categoria = request.form.get('categoria_nome_modal')
    tipo_categoria = request.form.get('categoria_tipo_modal')
    limite_categoria_str = request.form.get('categoria_limite', '').strip()
    
    # Debug para verificar os dados recebidos
    logging.info(f"Dados recebidos - Nome: '{nome_categoria}', Tipo: '{tipo_categoria}', Limite: '{limite_categoria_str}', Form completo: {dict(request.form)}")

    if not nome_categoria or not tipo_categoria:
        flash('Nome e Tipo da categoria são obrigatórios.', 'danger'); return redirect(redirect_url)

    valid_tipos = ['receita', 'gasto_variavel', 'gasto_fixo']
    if tipo_categoria not in valid_tipos:
        flash('Tipo de categoria inválido.', 'danger'); return redirect(redirect_url)

    # Processa o limite para gastos variáveis
    limite_valor = None
    if tipo_categoria == 'gasto_variavel' and limite_categoria_str:
        try:
            limite_categoria_clean = limite_categoria_str.replace(',', '.').replace(' ', '')
            limite_valor = Decimal(limite_categoria_clean)
            if limite_valor < 0:
                raise ValueError("O limite deve ser um valor positivo ou zero.")
        except (InvalidOperation, ValueError) as e:
            flash(f'Valor de límite inválido: {e}', 'danger')
            return redirect(redirect_url)

    conn = get_db_connection()
    if not conn:
        flash('Erro de conexão com o banco.', 'danger'); return redirect(redirect_url)

    cur = None
    try:
        cur = conn.cursor(cursor_factory=DictCursor)

        # Removido limite de criação de categorias

        # Insere COM o limite
        insert_query = sql.SQL("""
            INSERT INTO {schema}.categorias (nome, tipo, limite)
            VALUES (%s, %s, %s)
        """).format(schema=sql.Identifier(user_schema))
        cur.execute(insert_query, (nome_categoria.strip(), tipo_categoria, limite_valor))
        conn.commit()
        
        if limite_valor and limite_valor > 0:
            flash(f'¡Categoría "{nome_categoria}" agregada con límite de {format_currency_filter(limite_valor)}!', 'success')
        else:
            flash('¡Categoría agregada con éxito!', 'success')
        logging.info(f"Categoria '{nome_categoria}' ({tipo_categoria}) adicionada com limite {limite_valor} para schema {user_schema}")

    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        flash(f'Error: Ya existe una categoría con el nombre "{nome_categoria}" para el tipo "{tipo_categoria}".', 'danger')
        logging.warning(f"Tentativa de adicionar categoria duplicada: '{nome_categoria}' ({tipo_categoria}) para schema {user_schema}")
    except psycopg2.Error as e:
        conn.rollback()
        flash('Error.', 'danger')
        logging.error(f"Erro DB ao adicionar categoria (schema {user_schema}): {e}")
    except Exception as e:
        conn.rollback()
        flash('Error', 'danger')
        logging.error(f"Erro inesperado ao adicionar categoria (schema {user_schema}): {e}", exc_info=True)
    finally:
        if cur: cur.close()
        if conn: conn.close()

    return redirect(redirect_url)


# --- Rota para EDITAR Categoria (Atualizada SEM COR) ---
@app.route('/categorias/<int:categoria_id>/edit', methods=['POST'])
def edit_categoria(categoria_id):
    """Processa o formulário de edição de categoria, impedindo edição de fixas."""
    if 'user_assinatura_id' not in session:
        flash('Sessão expirada.', 'warning'); return redirect(url_for('login'))

    user_schema = session.get('user_schema')
    redirect_url = url_for('categorias')
    if not user_schema:
        flash('Erro interno.', 'danger'); session.clear(); return redirect(url_for('login'))

    nome_categoria = request.form.get('categoria_nome_modal')
    tipo_categoria = request.form.get('categoria_tipo_modal')
    limite_categoria_str = request.form.get('categoria_limite', '').strip()

    if not nome_categoria or not tipo_categoria:
        flash('Nome e Tipo são obrigatórios para editar.', 'danger'); return redirect(redirect_url)

    valid_tipos = ['receita', 'gasto_variavel', 'gasto_fixo']
    if tipo_categoria not in valid_tipos:
        flash('Tipo inválido.', 'danger'); return redirect(redirect_url)

    # Processa o limite para gastos variáveis
    limite_valor = None
    if tipo_categoria == 'gasto_variavel' and limite_categoria_str:
        try:
            limite_categoria_clean = limite_categoria_str.replace(',', '.').replace(' ', '')
            limite_valor = Decimal(limite_categoria_clean)
            if limite_valor < 0:
                raise ValueError("O limite deve ser um valor positivo ou zero.")
        except (InvalidOperation, ValueError) as e:
            flash(f'Valor de límite inválido: {e}', 'danger')
            return redirect(redirect_url)

    conn = get_db_connection()
    if not conn:
        flash('Erro de conexão.', 'danger'); return redirect(redirect_url)

    cur = None
    try:
        cur = conn.cursor(cursor_factory=DictCursor)

        # Atualiza incluindo o limite
        update_query = sql.SQL("""
            UPDATE {schema}.categorias
            SET nome = %s, tipo = %s, limite = %s, atualizado_em = NOW()
            WHERE id = %s
        """).format(schema=sql.Identifier(user_schema))
        cur.execute(update_query, (nome_categoria.strip(), tipo_categoria, limite_valor, categoria_id))
        conn.commit()

        if cur.rowcount > 0:
            flash('¡Categoría actualizada con éxito!', 'success')
            logging.info(f"Categoria ID {categoria_id} atualizada para '{nome_categoria}' ({tipo_categoria}) com limite {limite_valor} no schema {user_schema}")
        else:
            flash('Categoría no encontrada o no se pudo modificar.', 'warning')
            logging.warning(f"Edição para Categoria ID {categoria_id}: rowcount foi 0 (pós-verificação).")

    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        flash(f'Error: Ya existe otra categoría con el nombre "{nome_categoria}" para el tipo "{tipo_categoria}.', 'danger')
        logging.warning(f"Tentativa de editar para categoria duplicada: '{nome_categoria}' ({tipo_categoria}) ID:{categoria_id} no schema {user_schema}")
    except psycopg2.Error as e:
        conn.rollback()
        flash('Error', 'danger')
        logging.error(f"Erro DB ao editar categoria ID {categoria_id} (schema {user_schema}): {e}")
    except Exception as e:
        conn.rollback()
        flash('Error.', 'danger')
        logging.error(f"Erro inesperado ao editar categoria ID {categoria_id} (schema {user_schema}): {e}", exc_info=True)
    finally:
        if cur: cur.close()
        if conn: conn.close()

    return redirect(redirect_url)


# --- Rota para GERENCIAR LIMITE de Categoria ---
@app.route('/categorias/<int:categoria_id>/limite', methods=['POST'])
def set_limite_categoria(categoria_id):
    """Processa o formulário de definição/edição de limite para categoria de gasto variável."""
    if 'user_assinatura_id' not in session:
        flash('Sessão expirada.', 'warning'); return redirect(url_for('login'))

    user_schema = session.get('user_schema')
    redirect_url = url_for('categorias')
    if not user_schema:
        flash('Erro interno.', 'danger'); session.clear(); return redirect(url_for('login'))

    limite_valor_str = request.form.get('limite_valor', '').strip()
    
    # Debug: Log do valor recebido
    logging.info(f"Valor recebido para limite: '{limite_valor_str}' para categoria ID {categoria_id} no schema {user_schema}")

    conn = get_db_connection()
    if not conn:
        flash('Erro de conexão.', 'danger'); return redirect(redirect_url)

    cur = None
    try:
        cur = conn.cursor(cursor_factory=DictCursor)

        # Verifica se a categoria existe e é de gasto variável não fixa
        check_query = sql.SQL("""
            SELECT nome, tipo, is_fixa, limite
            FROM {schema}.categorias 
            WHERE id = %s
        """).format(schema=sql.Identifier(user_schema))
        cur.execute(check_query, (categoria_id,))
        categoria_info = cur.fetchone()

        if not categoria_info:
            flash('Categoria não encontrada.', 'warning')
            return redirect(redirect_url)

        if categoria_info['tipo'] != 'gasto_variavel':
            flash('Límites solo pueden ser definidos para categorías de Gasto Variable.', 'danger')
            return redirect(redirect_url)

        

        # Processa o valor do limite
        limite_valor = None
        if limite_valor_str and limite_valor_str != '':
            try:
                # Remove espaços e converte vírgula para ponto
                limite_valor_clean = limite_valor_str.replace(',', '.').replace(' ', '')
                limite_valor = Decimal(limite_valor_clean)
                if limite_valor < 0:
                    raise ValueError("O limite deve ser um valor positivo ou zero.")
                # Log do valor processado
                logging.info(f"Valor processado para limite: {limite_valor}")
            except (InvalidOperation, ValueError) as e:
                flash(f'Valor de límite inválido: {e}', 'danger')
                logging.error(f"Erro ao processar valor de limite '{limite_valor_str}': {e}")
                return redirect(redirect_url)

        # Atualiza o limite (NULL remove o limite, valor define o limite)
        update_query = sql.SQL("""
            UPDATE {schema}.categorias
            SET limite = %s
            WHERE id = %s
        """).format(schema=sql.Identifier(user_schema))
        
        # Log da query que será executada
        logging.info(f"Executando UPDATE limite: categoria_id={categoria_id}, limite_valor={limite_valor}")
        
        cur.execute(update_query, (limite_valor, categoria_id))
        conn.commit()

        # Verifica se a atualização foi bem-sucedida
        if cur.rowcount > 0:
            # Verifica o valor salvo no banco
            verify_query = sql.SQL("SELECT limite FROM {schema}.categorias WHERE id = %s").format(schema=sql.Identifier(user_schema))
            cur.execute(verify_query, (categoria_id,))
            resultado = cur.fetchone()
            valor_salvo = resultado['limite'] if resultado else None
            
            logging.info(f"Valor salvo no banco: {valor_salvo}")
            
            if limite_valor is not None and limite_valor > 0:
                flash(f'¡Límite de {format_currency_filter(limite_valor)} definido para "{categoria_info["nome"]}"!', 'success')
            else:
                flash(f'¡Límite removido de "{categoria_info["nome"]}"!', 'success')
            logging.info(f"Limite da categoria ID {categoria_id} atualizado para {limite_valor} no schema {user_schema}")
        else:
            flash('Categoria não encontrada ou não pôde ser modificada.', 'warning')
            logging.warning(f"UPDATE não afetou nenhuma linha para categoria ID {categoria_id}")

    except psycopg2.Error as e:
        conn.rollback()
        flash('Error en la base de datos.', 'danger')
        logging.error(f"Erro DB ao definir limite categoria ID {categoria_id} (schema {user_schema}): {e}")
    except Exception as e:
        conn.rollback()
        flash('Error inesperado.', 'danger')
        logging.error(f"Erro inesperado ao definir limite categoria ID {categoria_id} (schema {user_schema}): {e}", exc_info=True)
    finally:
        if cur: cur.close()
        if conn: conn.close()

    return redirect(redirect_url)


# --- Rota para EXCLUIR Categoria (sem alterações, já não usava cor) ---
@app.route('/categorias/<int:categoria_id>/delete', methods=['POST'])
def delete_categoria(categoria_id):
    """Processa a exclusão de uma categoria, impedindo exclusão de fixas."""
    if 'user_assinatura_id' not in session:
        flash('Sessão expirada.', 'warning'); return redirect(url_for('login'))

    user_schema = session.get('user_schema')
    redirect_url = url_for('categorias')
    if not user_schema:
        flash('Erro interno.', 'danger'); session.clear(); return redirect(url_for('login'))

    conn = get_db_connection()
    if not conn:
        flash('Erro de conexão.', 'danger'); return redirect(redirect_url)

    cur = None
    try:
        cur = conn.cursor(cursor_factory=DictCursor)

        # Busca informações da categoria
        check_query = sql.SQL("SELECT nome FROM {schema}.categorias WHERE id = %s").format(schema=sql.Identifier(user_schema))
        cur.execute(check_query, (categoria_id,))
        categoria_info = cur.fetchone()

        if not categoria_info:
             flash('Categoria não encontrada.', 'warning')
             return redirect(redirect_url)

        # Verifica uso da categoria
        categoria_nome = categoria_info['nome']
        check_usage_query = sql.SQL("""
            SELECT
                (SELECT COUNT(*) FROM {schema}.gastos WHERE categoria = %s) +
                (SELECT COUNT(*) FROM {schema}.gastos_fixos WHERE categoria = %s) +
                (SELECT COUNT(*) FROM {schema}.outras_receitas WHERE categoria = %s)
            AS total_uso
        """).format(schema=sql.Identifier(user_schema))

        cur.execute(check_usage_query, (categoria_nome, categoria_nome, categoria_nome))
        usage = cur.fetchone()

        if usage and usage['total_uso'] > 0:
            flash(f'Erro: A categoria "{categoria_nome}" está sendo usada em {usage["total_uso"]} transações e não pode ser excluída.', 'danger')
            return redirect(redirect_url)

        # Exclui (se não estiver em uso)
        delete_query = sql.SQL("DELETE FROM {schema}.categorias WHERE id = %s").format(schema=sql.Identifier(user_schema))
        cur.execute(delete_query, (categoria_id,))
        conn.commit()

        if cur.rowcount > 0:
            flash('¡Categoría eliminada con éxito!', 'success')
            logging.info(f"Categoria ID {categoria_id} ('{categoria_nome}') excluída do schema {user_schema}")
        else:
            flash('Categoría no encontrada o no se pudo eliminar.', 'warning')
            logging.warning(f"Tentativa de excluir categoria ID {categoria_id} não encontrada/não excluída (pós-verificações) no schema {user_schema}")

    except psycopg2.Error as e:
        conn.rollback()
        flash('Error.', 'danger')
        logging.error(f"Erro DB ao excluir categoria ID {categoria_id} (schema {user_schema}): {e}")
    except Exception as e:
        conn.rollback()
        flash('Error.', 'danger')
        logging.error(f"Erro inesperado ao excluir categoria ID {categoria_id} (schema {user_schema}): {e}", exc_info=True)
    finally:
        if cur: cur.close()
        if conn: conn.close()

    return redirect(redirect_url)





# --- Rota para Adicionar OU Editar Lembrete ---




@app.route('/lembretes/save', methods=['POST']) # Rota unificada para salvar
def save_lembrete():
    if 'user_assinatura_id' not in session:
        flash('Sessão expirada. Por favor, faça login novamente.', 'warning')
        return redirect(url_for('login'))

    user_schema = session.get('user_schema')
    redirect_url = url_for('lembretes') # Redireciona de volta para a lista de lembretes

    if not user_schema:
        flash('Erro interno: Schema do usuário não encontrado.', 'danger')
        session.clear()
        return redirect(url_for('login'))

    # Obter dados do formulário
    lembrete_id = request.form.get('lembrete_id') # ID virá do input hidden se for edição
    descricao = request.form.get('descricao_lembrete')
    data_str = request.form.get('data_lembrete')
    valor_str = request.form.get('valor_lembrete', '').strip()

    # --- Tratamento do Valor ---
    valor_decimal = Decimal('0.00')  # Valor padrão
    if valor_str:
        try:
            valor_decimal = Decimal(valor_str.replace(',', '.'))
            if valor_decimal < 0:
                raise ValueError("O valor deve ser positivo ou zero.")
        except (InvalidOperation, ValueError) as e:
            flash(f'Erro: Valor inválido ({valor_str}). {e}', 'danger')
            logging.warning(f"Salvar lembrete falhou: Valor inválido '{valor_str}'. Schema: {user_schema}")
            return redirect(redirect_url)

    # --- Tratamento da Repetição ---
    repetir_str = request.form.get('repetir_lembrete', 'false') # Pega 'false' se não vier
    repetir = repetir_str.lower() == 'true' # Converte para booleano de forma segura
    tipo_rep = None # Inicializa como None

    if repetir:
        tipo_rep_form = request.form.get('tipo_repeticion_lembrete')
        # Validação básica: por enquanto, apenas 'mensal' é suportado ou será o padrão.
        if tipo_rep_form and tipo_rep_form.lower() == 'mensal':
            tipo_rep = 'mensal'
        else:
            # Define 'mensal' como padrão se 'repetir' é true mas o tipo é inválido/ausente.
            tipo_rep = 'mensal'
            logging.warning(
                f"Tipo de repetição inválido/ausente ('{tipo_rep_form}') ao salvar lembrete "
                f"(ID: {lembrete_id or 'Novo'}). Assumindo 'mensal'. Schema: {user_schema}"
            )
    # Se 'repetir' for False, tipo_rep permanecerá None.

    # Validação básica dos campos obrigatórios
    if not descricao or not data_str:
        flash('Erro: Descrição e Data são obrigatórias.', 'danger')
        logging.warning(f"Salvar lembrete falhou: Descrição ou Data ausentes. Schema: {user_schema}")
        return redirect(redirect_url)

    # Validação e conversão da Data
    try:
        data_obj = datetime.strptime(data_str, '%Y-%m-%d').date()
    except ValueError:
        flash('Erro: Formato de data inválido. Use AAAA-MM-DD.', 'danger')
        logging.warning(f"Salvar lembrete falhou: Data inválida '{data_str}'. Schema: {user_schema}")
        return redirect(redirect_url)

    # Conexão com o banco
    conn = get_db_connection()
    if not conn:
        flash('Erro: Falha ao conectar com o banco de dados.', 'danger')
        return redirect(redirect_url)

    cur = None
    try:
        cur = conn.cursor()
        if lembrete_id and lembrete_id.isdigit():
            # --- Modo Edição ---
            lembrete_id_int = int(lembrete_id)
            logging.info(f"Tentativa de editar Lembrete ID {lembrete_id_int} no schema {user_schema}")
            update_query = sql.SQL("""
                UPDATE {schema}.lembretes
                SET descripcion = %s, data = %s, valor = %s, repetir = %s, tipo_repeticion = %s
                WHERE id = %s
            """).format(schema=sql.Identifier(user_schema))
            # Parâmetros na ordem correta: descricao, data, valor, repetir, tipo_rep, id
            query_params = (descricao, data_obj, valor_decimal, repetir, tipo_rep, lembrete_id_int)

            logging.debug(f"Executando UPDATE Lembrete: Query={update_query.as_string(conn)} Params={query_params}")
            cur.execute(update_query, query_params)
            conn.commit()
            if cur.rowcount > 0:
                flash('¡Recordatorio actualizado con éxito!', 'success')
                logging.info(f"Lembrete ID {lembrete_id_int} atualizado com sucesso (repetir={repetir}, tipo={tipo_rep}). Schema: {user_schema}.")
            else:
                flash('Recordatorio no encontrado o ningún dato fue modificado.', 'warning')
                logging.warning(f"Edição para Lembrete ID {lembrete_id_int}: rowcount foi 0.")
        else:
            # --- Modo Adição ---
            logging.info(f"Tentativa de adicionar novo Lembrete no schema {user_schema}")
            insert_query = sql.SQL("""
                INSERT INTO {schema}.lembretes (descripcion, data, valor, repetir, tipo_repeticion)
                VALUES (%s, %s, %s, %s, %s)
            """).format(schema=sql.Identifier(user_schema))
            # Parâmetros na ordem correta: descricao, data, valor, repetir, tipo_rep
            query_params = (descricao, data_obj, valor_decimal, repetir, tipo_rep)

            logging.debug(f"Executando INSERT Lembrete: Query={insert_query.as_string(conn)} Params={query_params}")
            cur.execute(insert_query, query_params)
            conn.commit()
            flash('¡Recordatorio agregado con éxito!', 'success')
            logging.info(f"Novo lembrete '{descricao}' adicionado com sucesso (repetir={repetir}, tipo={tipo_rep}). Schema: {user_schema}.")

    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash(f'Erro no banco de dados: {e}', 'danger')
        logging.error(f"Erro DB (psycopg2) ao salvar Lembrete ID {lembrete_id or 'Novo'} schema {user_schema}: {e}")
    except Exception as e:
        if conn: conn.rollback()
        flash(f'Ocorreu um erro inesperado.', 'danger')
        logging.error(f"Erro inesperado (Python) ao salvar Lembrete ID {lembrete_id or 'Novo'} schema {user_schema}: {e}", exc_info=True)
    finally:
        if cur: cur.close()
        if conn: conn.close()

    return redirect(redirect_url)




@app.route('/receitas/<int:item_id>/delete', methods=['POST'])
def delete_outra_receita(item_id):
    """
    Processa a solicitação de exclusão de uma 'Outra Receita'.
    """
    # 1. Verifica se o usuário está logado
    if 'user_assinatura_id' not in session:
        flash('Sessão expirada. Por favor, faça login novamente.', 'warning')
        return redirect(url_for('login'))

    # 2. Obtém o schema do usuário da sessão
    user_schema = session.get('user_schema')
    redirect_url = url_for('receitas') # URL para redirecionar após a exclusão

    # 3. Valida se o schema existe
    if not user_schema:
        flash('Erro interno: Schema do usuário não encontrado.', 'danger')
        session.clear() # Limpa a sessão inválida
        return redirect(url_for('login'))

    logging.info(f"Tentativa de excluir Outra Receita ID {item_id} no schema {user_schema}")

    # 4. Conecta ao banco de dados
    conn = get_db_connection()
    if not conn:
        flash('Erro: Falha ao conectar com o banco de dados.', 'danger')
        return redirect(redirect_url)

    # 5. Executa a exclusão
    cur = None
    try:
        cur = conn.cursor()
        # Monta a query DELETE de forma segura
        delete_query = sql.SQL("DELETE FROM {schema}.outras_receitas WHERE id = %s").format(
            schema=sql.Identifier(user_schema)
        )
        logging.debug(f"Executando DELETE Outra Receita: Query={delete_query.as_string(conn)} Params={[item_id]}")
        # Executa a query passando o ID como parâmetro
        cur.execute(delete_query, (item_id,))
        # Confirma a transação
        conn.commit()

        # 6. Verifica se a exclusão foi bem-sucedida
        if cur.rowcount > 0:
            flash('¡Entrada eliminada con éxito!', 'success')
            logging.info(f"Outra Receita ID {item_id} excluída com sucesso do schema {user_schema}.")
        else:
            # Se rowcount for 0, o ID não existia na tabela
            flash('Entrada no encontrada. Es posible que ya haya sido eliminada.', 'warning')
            logging.warning(f"Exclusão para Outra Receita ID {item_id}: rowcount foi 0.")

    except psycopg2.Error as e:
        # Em caso de erro do banco, desfaz a transação e loga
        if conn: conn.rollback()
        flash(f'Error {e}', 'danger')
        logging.error(f"Erro DB (psycopg2) ao excluir Outra Receita ID {item_id} schema {user_schema}: {e}")
    except Exception as e:
        # Em caso de outro erro, desfaz a transação e loga
        if conn: conn.rollback()
        flash(f'Error', 'danger')
        logging.error(f"Erro inesperado (Python) ao excluir Outra Receita ID {item_id} schema {user_schema}: {e}", exc_info=True)
    finally:
        # Garante o fechamento da conexão e cursor
        if cur: cur.close()
        if conn: conn.close()

    # 7. Redireciona de volta para a página de receitas
    return redirect(redirect_url)




# --- Rota para Excluir Outra Receita ---
@app.route('/lembretes/<int:item_id>/delete', methods=['POST'])
def delete_lembrete(item_id):
    """
    Processa a solicitação de exclusão de um Lembrete.
    """
    if 'user_assinatura_id' not in session:
        flash('Sessão expirada. Por favor, faça login novamente.', 'warning')
        return redirect(url_for('login'))

    user_schema = session.get('user_schema')
    redirect_url = url_for('lembretes') # Redireciona de volta para a lista

    if not user_schema:
        flash('Erro interno: Schema do usuário não encontrado.', 'danger')
        session.clear()
        return redirect(url_for('login'))

    logging.info(f"Tentativa de excluir Lembrete ID {item_id} no schema {user_schema}")

    conn = get_db_connection()
    if not conn:
        flash('Erro: Falha ao conectar com o banco de dados.', 'danger')
        return redirect(redirect_url)

    cur = None
    try:
        cur = conn.cursor()
        delete_query = sql.SQL("DELETE FROM {schema}.lembretes WHERE id = %s").format(
            schema=sql.Identifier(user_schema)
        )
        logging.debug(f"Executando DELETE Lembrete: Query={delete_query.as_string(conn)} Params={[item_id]}")
        cur.execute(delete_query, (item_id,))
        conn.commit()

        if cur.rowcount > 0:
            flash('¡Recordatorio eliminado con éxito!', 'success')
            logging.info(f"Lembrete ID {item_id} excluído com sucesso do schema {user_schema}.")
        else:
            flash('Recordatorio no encontrado. Es posible que ya haya sido eliminado.', 'warning')
            logging.warning(f"Exclusão para Lembrete ID {item_id}: rowcount foi 0.")

    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash(f'Error: {e}', 'danger')
        logging.error(f"Erro DB (psycopg2) ao excluir Lembrete ID {item_id} schema {user_schema}: {e}")
    except Exception as e:
        if conn: conn.rollback()
        flash(f'Error', 'danger')
        logging.error(f"Erro inesperado (Python) ao excluir Lembrete ID {item_id} schema {user_schema}: {e}", exc_info=True)
    finally:
        if cur: cur.close()
        if conn: conn.close()

    return redirect(redirect_url)


@app.route('/gastos/<string:tipo_gasto>/<int:item_id>/edit', methods=['POST'])
def edit_gasto(tipo_gasto, item_id):
    """
    Processa a submissão do formulário de edição de um gasto (variável ou fixo).
    - Lógica de 'activo' REMOVIDA para gastos fixos. Eles são sempre ativos.
    """
    if 'user_assinatura_id' not in session:
        flash('Sessão expirada. Por favor, faça login novamente.', 'warning')
        return redirect(url_for('login'))

    user_schema = session.get('user_schema')
    redirect_url = url_for('gastos', tipo=tipo_gasto)

    if not user_schema:
        flash('Erro interno: Schema do usuário não encontrado.', 'danger')
        session.clear()
        return redirect(url_for('login'))

    logging.info(f"Tentativa de editar gasto ID {item_id} (Tipo: {tipo_gasto}) no schema {user_schema}")

    if tipo_gasto == 'variaveis':
        table_name = sql.Identifier('gastos')
        descricao_form = request.form.get('descricao')
        valor_str = request.form.get('valor')
        categoria_form = request.form.get('categoria')
        data_str = request.form.get('data')
        metodo_pagamento_form = request.form.get('metodo_pagamento')
        metodo_pagamento_id_final = int(metodo_pagamento_form) if metodo_pagamento_form and metodo_pagamento_form.isdigit() else None
        
        update_values = {
            'descripcion': descricao_form, 'valor': None, 'categoria': categoria_form,
            'data': None, 'metodo_pagamento_id': metodo_pagamento_id_final
        }
        required_fields_check = [descricao_form, valor_str, categoria_form, data_str]
        
    elif tipo_gasto == 'fixos':
        table_name = sql.Identifier('gastos_fixos')
        descricao_form = request.form.get('descricao')
        valor_str = request.form.get('valor')
        categoria_form = request.form.get('categoria')
        data_str = request.form.get('fecha_inicio_fixo')
        
        # --- ALTERAÇÃO AQUI ---
        # Removemos completamente a leitura e o processamento do campo 'activo'.
        update_values = {
            'descripcion': descricao_form, 'valor': None, 'categoria': categoria_form,
            'fecha_inicio': None, 'recurrencia': 'mensual'
        }
        required_fields_check = [descricao_form, valor_str, categoria_form, data_str]

    else:
        flash('Tipo de gasto inválido.', 'danger')
        return redirect(url_for('gastos'))

    # Validações (permanecem as mesmas)
    if not all(field is not None and field.strip() != '' for field in required_fields_check):
        flash('Erro: Preencha todos os campos obrigatórios.', 'danger')
        return redirect(redirect_url)

    try:
        valor_decimal = Decimal(valor_str.replace(',', '.'))
        if valor_decimal <= 0: raise ValueError("O valor deve ser positivo.")
        update_values['valor'] = valor_decimal
    except (InvalidOperation, ValueError) as e:
        flash(f'Erro: Valor monetário inválido. {e}', 'danger')
        return redirect(redirect_url)

    try:
        data_obj = datetime.strptime(data_str, '%Y-%m-%d').date()
        if tipo_gasto == 'variaveis':
            update_values['data'] = data_obj
        else:
            update_values['fecha_inicio'] = data_obj
    except (ValueError, TypeError):
        flash('Erro: Formato de data inválido.', 'danger')
        return redirect(redirect_url)
    
    conn = get_db_connection()
    if not conn:
        flash('Erro de conexão com o banco de dados.', 'danger')
        return redirect(redirect_url)
        
    # Lógica de atualização (agora não inclui mais o campo 'activo')
    cur = None
    try:
        cur = conn.cursor()
        set_clauses = []
        query_params = []
        # O campo 'activo' não estará mais em update_values para gastos fixos,
        # então ele não será incluído na query UPDATE.
        for col, val in update_values.items():
            if val is not None:
                 set_clauses.append(sql.SQL("{} = %s").format(sql.Identifier(col)))
                 query_params.append(val)

        if not set_clauses:
             flash('Nenhum dado válido fornecido para atualização.', 'warning')
        else:
            query_params.append(item_id)
            update_query = sql.SQL("UPDATE {schema}.{table} SET {set_sql} WHERE id = %s").format(
                schema=sql.Identifier(user_schema),
                table=table_name,
                set_sql=sql.SQL(', ').join(set_clauses)
            )
            cur.execute(update_query, query_params)
            conn.commit()

            if cur.rowcount > 0:
                flash('¡Gasto actualizado con éxito!', 'success')
            else:
                flash('Gasto no encontrado o ningún dato fue modificado.', 'warning')

    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash(f'Error: {e}', 'danger')
    finally:
        if cur: cur.close()
        if conn: conn.close()

    return redirect(redirect_url)


@app.route('/gastos/<string:tipo_gasto>/<int:item_id>/delete', methods=['POST'])
def delete_gasto(tipo_gasto, item_id):
    """
    Processa a solicitação de exclusão de um gasto (variável ou fixo).
    """
    # Verifica se o usuário está logado
    if 'user_assinatura_id' not in session:
        flash('Sessão expirada. Por favor, faça login novamente.', 'warning')
        return redirect(url_for('login'))

    user_schema = session.get('user_schema')
    # URL para redirecionar após a ação
    redirect_url = url_for('gastos', tipo=tipo_gasto)

    # Verifica o schema
    if not user_schema:
        flash('Erro interno: Schema do usuário não encontrado.', 'danger')
        session.clear()
        return redirect(url_for('login'))

    logging.info(f"Tentativa de excluir gasto ID {item_id} (Tipo: {tipo_gasto}) no schema {user_schema}")

    # 1. Determina a tabela correta com base no tipo_gasto
    if tipo_gasto == 'variaveis':
        table_name = sql.Identifier('gastos')
    elif tipo_gasto == 'fixos':
        table_name = sql.Identifier('gastos_fixos')
    else:
        # Se o tipo_gasto na URL for inválido
        flash('Tipo de gasto inválido.', 'danger')
        logging.warning(f"Tipo de gasto inválido '{tipo_gasto}' na URL para exclusão do ID {item_id}.")
        return redirect(url_for('gastos')) # Vai para a página padrão

    # 2. Conecta ao banco e executa o DELETE
    conn = get_db_connection()
    if not conn:
        flash('Erro: Falha ao conectar com o banco de dados.', 'danger')
        return redirect(redirect_url)

    cur = None
    try:
        cur = conn.cursor()
        # Constrói a query DELETE de forma segura
        delete_query = sql.SQL("DELETE FROM {schema}.{table} WHERE id = %s").format(
            schema=sql.Identifier(user_schema),
            table=table_name
        )
        logging.debug(f"Executando DELETE: Query={delete_query.as_string(conn)} Params={[item_id]}")
        # Executa a query passando o ID como parâmetro
        cur.execute(delete_query, (item_id,))
        # Confirma a transação
        conn.commit()

        # Verifica se a exclusão realmente afetou alguma linha
        if cur.rowcount > 0:
            flash('¡Gasto eliminado con éxito!', 'success')
            logging.info(f"Gasto ID {item_id} (Tipo: {tipo_gasto}) excluído com sucesso do schema {user_schema}.")
        else:
            # Se rowcount for 0, o ID não existia na tabela
            flash('Gasto no encontrado. Es posible que ya haya sido eliminado anteriormente.', 'warning')
            logging.warning(f"Exclusão para ID {item_id} ({tipo_gasto}): rowcount foi 0 (item não encontrado).")

    except psycopg2.Error as e:
        # Em caso de erro do banco, desfaz a transação e loga
        if conn: conn.rollback()
        flash(f'Error {e}', 'danger')
        logging.error(f"Erro DB (psycopg2) ao excluir gasto {tipo_gasto} ID {item_id} schema {user_schema}: {e}")
    except Exception as e:
        # Em caso de outro erro, desfaz a transação e loga
        if conn: conn.rollback()
        flash(f'Error', 'danger')
        logging.error(f"Erro inesperado (Python) ao excluir gasto {tipo_gasto} ID {item_id} schema {user_schema}: {e}", exc_info=True)
    finally:
        # Garante o fechamento da conexão e cursor
        if cur: cur.close()
        if conn: conn.close()

    # Redireciona o usuário de volta para a lista
    return redirect(redirect_url)


ITEMS_PER_PAGE = 30


@app.route('/dashboard')
@cache.cached(timeout=300)  # Cache for 5 minutes
def dashboard():
    if 'user_assinatura_id' not in session:
        flash('Você precisa fazer login para acessar esta página.', 'warning')
        return redirect(url_for('login'))

    user_schema = session.get('user_schema')
    user_nome = session.get('user_nome', session.get('user_email'))

    if not user_schema:
        logging.error(f"Schema não encontrado na sessão para usuário {user_nome} (ID: {session.get('user_assinatura_id')}) ao acessar /dashboard. Deslogando.")
        flash('Erro interno: Informações do usuário incompletas. Por favor, faça login novamente.', 'danger')
        session.clear()
        return redirect(url_for('login'))

    # --- INÍCIO DA LÓGICA DE SELEÇÃO DE PERÍODO ---
    periodo_selecionado = request.args.get('periodo', 'mes_atual')
    hoje = date.today()
    data_fim_periodo = hoje

    if periodo_selecionado == 'mes_atual':
        data_inicio_periodo = hoje.replace(day=1)
    elif periodo_selecionado == '15d':
        data_inicio_periodo = hoje - timedelta(days=14)
    elif periodo_selecionado == '7d':
        data_inicio_periodo = hoje - timedelta(days=6)
    else: # Fallback
        periodo_selecionado = 'mes_atual'
        data_inicio_periodo = hoje.replace(day=1)

    logging.info(f"Acessando dashboard: Schema {user_schema}, Período: {periodo_selecionado} ({data_inicio_periodo} a {data_fim_periodo})")
    # --- FIM DA LÓGICA DE SELEÇÃO DE PERÍODO ---

    dados = {
        "total_receitas_mes": Decimal('0.00'),
        "total_despesas_mes": Decimal('0.00'),
        "saldo_mes": Decimal('0.00'),
        "movimentacoes_recentes": [],
        "proximos_lembretes": [],
        "gastos_categoria_labels": [],
        "gastos_categoria_data": [],
        "gastos_fixos_categoria_labels": [],
        "gastos_fixos_categoria_data": [],
        "gastos_tempo_labels": [],
        "gastos_tempo_data": []
    }
    meta_ativa = None
    categorias_por_tipo = {
        'receita': [],
        'gasto_variavel': [],
        'gasto_fixo': []
    }
    metodos_pagamento_disponiveis = []

    conn = get_db_connection()
    if not conn:
        flash('Erro de conexão com o banco ao carregar dashboard.', 'danger')
        dados_json_string = json.dumps({
            "gastos_categoria_labels": [], "gastos_categoria_data": [],
            "gastos_tempo_labels": [], "gastos_tempo_data": []
        }, default=json_converter)
        return render_template('dashboard.html', user_nome=user_nome, dados=dados, meta_ativa=meta_ativa,
                               dados_json=dados_json_string, categorias_por_tipo=categorias_por_tipo,
                               periodo_ativo=periodo_selecionado) # Passa periodo_ativo no fallback

    cur = None
    try:
        cur = conn.cursor(cursor_factory=DictCursor)

        for tipo_cat_loop in categorias_por_tipo.keys():
            categorias_por_tipo[tipo_cat_loop] = buscar_categorias_por_tipo(conn, user_schema, tipo_cat_loop)
        
        # Buscar métodos de pagamento ativos para os formulários
        metodos_pagamento_disponiveis = buscar_metodos_pagamento_ativos(conn, user_schema)

        # --- Buscar Dados Financeiros para o PERÍODO SELECIONADO ---
        
        # Inicializar variáveis importantes
        gastos_fixos_ativos = []
        gastos_metodo_labels = []
        gastos_metodo_data = []
        
        # Gerar lista de dias no período
        dias_no_periodo_list = []
        if data_inicio_periodo <= data_fim_periodo:
            query_dias = sql.SQL("SELECT generate_series(%s::date, %s::date, '1 day'::interval)::date as dia")
            cur.execute(query_dias, (data_inicio_periodo, data_fim_periodo))
            dias_no_periodo_list = [r['dia'] for r in cur.fetchall()]

        # 1. Receitas do Período (Salário da config já foi removido)
        soma_outras_receitas_periodo = Decimal('0.00')
        query_soma_outras_receitas = sql.SQL(
            "SELECT COALESCE(SUM(valor), 0) as total_outras_receitas FROM {schema}.outras_receitas "
            "WHERE fecha BETWEEN %s AND %s"
        ).format(schema=sql.Identifier(user_schema))
        cur.execute(query_soma_outras_receitas, (data_inicio_periodo, data_fim_periodo))
        outras_receitas_result = cur.fetchone()
        if outras_receitas_result and outras_receitas_result['total_outras_receitas'] is not None:
            soma_outras_receitas_periodo = outras_receitas_result['total_outras_receitas']

        dados['total_receitas_mes'] = soma_outras_receitas_periodo

        # 2. Gastos Variáveis do Período
        total_gastos_variaveis_periodo = Decimal('0.00')
        query_total_gastos_variaveis = sql.SQL(
            "SELECT COALESCE(SUM(valor), 0) as total_gastos_mes FROM {schema}.gastos "
            "WHERE data BETWEEN %s AND %s"
        ).format(schema=sql.Identifier(user_schema))
        cur.execute(query_total_gastos_variaveis, (data_inicio_periodo, data_fim_periodo))
        total_gastos_var_result = cur.fetchone()
        if total_gastos_var_result and total_gastos_var_result['total_gastos_mes'] is not None:
            total_gastos_variaveis_periodo = total_gastos_var_result['total_gastos_mes']

        # 3. Calcular Total de Gastos Fixos do Período
        total_gastos_fixos_periodo = Decimal('0.00')
        query_base_fixos_dash = sql.SQL(
            "SELECT id, fecha_inicio, valor, recurrencia FROM {schema}.gastos_fixos "
            "WHERE activo = TRUE AND fecha_inicio <= %s" 
        ).format(schema=sql.Identifier(user_schema))
        cur.execute(query_base_fixos_dash, (data_fim_periodo,))
        gastos_fixos_ativos_para_periodo = cur.fetchall()

        for gf_fixo in gastos_fixos_ativos_para_periodo:
            rrule_params = get_rrule_params(gf_fixo['recurrencia'])
            if rrule_params:
                try:
                    dtstart_gf_fixo = gf_fixo['fecha_inicio']
                    if isinstance(dtstart_gf_fixo, datetime):
                        dtstart_gf_fixo = dtstart_gf_fixo.date()

                    occurrences = list(rrule(dtstart=dtstart_gf_fixo, until=data_fim_periodo, **rrule_params))
                    for occ_dt in occurrences:
                        occ_date = occ_dt.date()
                        if data_inicio_periodo <= occ_date <= data_fim_periodo:
                            total_gastos_fixos_periodo += gf_fixo['valor']
                except Exception as e_rrule:
                    logging.error(f"Dashboard Gasto Fixo ID {gf_fixo['id']} rrule error: {e_rrule} para o período {data_inicio_periodo} a {data_fim_periodo}")
            else: 
                if gf_fixo['recurrencia'].lower().strip() in ['unico', 'único', 'única']:
                    if data_inicio_periodo <= gf_fixo['fecha_inicio'] <= data_fim_periodo:
                        total_gastos_fixos_periodo += gf_fixo['valor']

        logging.info(f"Dashboard: Gastos Fixos (período {data_inicio_periodo}-{data_fim_periodo}): {total_gastos_fixos_periodo}")

        # 4. Calcular Totais e Saldo para o período
        dados['total_despesas_mes'] = total_gastos_variaveis_periodo + total_gastos_fixos_periodo
        dados['saldo_mes'] = dados['total_receitas_mes'] - dados['total_despesas_mes']
        
        # 5. Calcular limite diário para poupança (70% das receitas dividido por 30 dias)
        if dados['total_receitas_mes'] > 0:
            dados['limite_diario_poupanca'] = (dados['total_receitas_mes'] * Decimal('0.7')) / Decimal('30')
        else:
            dados['limite_diario_poupanca'] = Decimal('0.00')

        # --- Buscar Movimentações Recentes (Mantido como antes - os últimos X, independente do período do card) ---
        movimentacoes = []
        query_gastos_ultimos = sql.SQL(
            "SELECT id, data, descripcion, valor, categoria FROM {schema}.gastos "
            "ORDER BY data DESC, id DESC LIMIT 3"
        ).format(schema=sql.Identifier(user_schema))
        cur.execute(query_gastos_ultimos)
        ultimos_gastos = cur.fetchall()
        for g_mov in ultimos_gastos:
            movimentacoes.append({
                'id': g_mov['id'], 'data': g_mov['data'], 'descricao': g_mov['descripcion'],
                'valor': g_mov['valor'], 'categoria': g_mov['categoria'], 'tipo_movimentacao': 'gasto_variavel'
            })

        query_gastos_fixos_ultimos = sql.SQL(
            "SELECT id, fecha_inicio as data, descripcion, valor, categoria FROM {schema}.gastos_fixos "
            "WHERE activo = TRUE ORDER BY fecha_inicio DESC, id DESC LIMIT 2"
        ).format(schema=sql.Identifier(user_schema))
        cur.execute(query_gastos_fixos_ultimos)
        ultimos_gastos_fixos = cur.fetchall()
        for gf_mov in ultimos_gastos_fixos:
            movimentacoes.append({
                'id': gf_mov['id'], 'data': gf_mov['data'], 'descricao': gf_mov['descripcion'],
                'valor': gf_mov['valor'], 'categoria': gf_mov['categoria'], 'tipo_movimentacao': 'gasto_fixo'
            })

        query_receitas_ultimas = sql.SQL(
            "SELECT id, fecha as data, descripcion, valor, categoria FROM {schema}.outras_receitas "
            "ORDER BY fecha DESC, id DESC LIMIT 2"
        ).format(schema=sql.Identifier(user_schema))
        cur.execute(query_receitas_ultimas)
        ultimas_outras_receitas = cur.fetchall()
        for r_mov in ultimas_outras_receitas:
            movimentacoes.append({
                'id': r_mov['id'], 'data': r_mov['data'], 'descricao': r_mov['descripcion'],
                'valor': r_mov['valor'], 'categoria': r_mov['categoria'], 'tipo_movimentacao': 'receita'
            })
        # Filtrar movimientos sin fecha y ordenar
        movimentacoes = [m for m in movimentacoes if m.get('data') is not None]
        movimentacoes.sort(key=lambda x: x['data'], reverse=True)
        dados['movimentacoes_recentes'] = movimentacoes[:5]

        # --- Buscar Próximos Lembretes (Não depende do período do card) ---
        query_lembretes_dashboard = sql.SQL(
            "SELECT id, descripcion, data, valor FROM {schema}.lembretes "
            "WHERE data >= CURRENT_DATE ORDER BY data ASC LIMIT 5"
        ).format(schema=sql.Identifier(user_schema))
        cur.execute(query_lembretes_dashboard)
        dados['proximos_lembretes'] = [dict(l_rem) for l_rem in cur.fetchall()]

        # --- Buscar Dados para Gráficos ---
        # Gráfico de Gastos por Categoria (MANTIDO MENSAL POR SIMPLICIDADE INICIAL)
        query_gastos_cat_chart = sql.SQL("""
            SELECT categoria, SUM(valor) as total 
            FROM {schema}.gastos
            WHERE data BETWEEN %(data_inicio)s AND %(data_fim)s
            AND categoria IS NOT NULL 
            GROUP BY categoria 
            ORDER BY total DESC
        """).format(schema=sql.Identifier(user_schema))
        cur.execute(query_gastos_cat_chart, {'data_inicio': data_inicio_periodo, 'data_fim': data_fim_periodo})
        gastos_por_categoria_chart = cur.fetchall()
        dados['gastos_categoria_labels'] = [g_cat['categoria'] for g_cat in gastos_por_categoria_chart]
        dados['gastos_categoria_data'] = [g_cat['total'] for g_cat in gastos_por_categoria_chart]

        # Gráfico de Gastos Fixos por Categoria (baseado no período selecionado)
        gastos_fixos_por_categoria_periodo = {}
        
        # Buscar gastos fixos ativos para calcular por categoria no período
        query_gastos_fixos_periodo_cat = sql.SQL("""
            SELECT id, categoria, valor, fecha_inicio, recurrencia 
            FROM {schema}.gastos_fixos 
            WHERE activo = TRUE AND fecha_inicio <= %s
            AND categoria IS NOT NULL
        """).format(schema=sql.Identifier(user_schema))
        cur.execute(query_gastos_fixos_periodo_cat, (data_fim_periodo,))
        gastos_fixos_ativos_cat = cur.fetchall()
        
        logging.info(f"Processando {len(gastos_fixos_ativos_cat)} gastos fixos para categoria no período {data_inicio_periodo} - {data_fim_periodo}")
        
        for gf_cat in gastos_fixos_ativos_cat:
            categoria_gf = gf_cat['categoria']
            if categoria_gf not in gastos_fixos_por_categoria_periodo:
                gastos_fixos_por_categoria_periodo[categoria_gf] = Decimal('0.00')
                
            rrule_params = get_rrule_params(gf_cat['recurrencia'])
            if rrule_params:
                try:
                    dtstart_gf_cat = gf_cat['fecha_inicio']
                    if isinstance(dtstart_gf_cat, datetime):
                        dtstart_gf_cat = dtstart_gf_cat.date()

                    occurrences = list(rrule(dtstart=dtstart_gf_cat, until=data_fim_periodo, **rrule_params))
                    for occ_dt in occurrences:
                        occ_date = occ_dt.date()
                        if data_inicio_periodo <= occ_date <= data_fim_periodo:
                            gastos_fixos_por_categoria_periodo[categoria_gf] += gf_cat['valor']
                            logging.debug(f"Gasto fixo ID {gf_cat['id']} categoria {categoria_gf}: +{gf_cat['valor']} em {occ_date}")
                except Exception as e_rrule:
                    logging.error(f"Dashboard Gasto Fixo Categoria ID {gf_cat.get('id', 'N/A')} rrule error: {e_rrule}")
            else:
                if gf_cat['recurrencia'].lower().strip() in ['unico', 'único', 'única']:
                    if data_inicio_periodo <= gf_cat['fecha_inicio'] <= data_fim_periodo:
                        gastos_fixos_por_categoria_periodo[categoria_gf] += gf_cat['valor']
                        logging.debug(f"Gasto fixo único ID {gf_cat['id']} categoria {categoria_gf}: +{gf_cat['valor']} em {gf_cat['fecha_inicio']}")
        
        # Ordenar por valor total e filtrar valores maiores que zero
        gastos_fixos_ordenados = sorted(gastos_fixos_por_categoria_periodo.items(), key=lambda x: x[1], reverse=True)
        dados['gastos_fixos_categoria_labels'] = [cat for cat, val in gastos_fixos_ordenados if val > 0]
        dados['gastos_fixos_categoria_data'] = [float(val) for cat, val in gastos_fixos_ordenados if val > 0]
        
        logging.info(f"Gastos fixos por categoria (período): {len(dados['gastos_fixos_categoria_labels'])} categorias encontradas com valores: {dict(zip(dados['gastos_fixos_categoria_labels'], dados['gastos_fixos_categoria_data']))}")

        # Gráfico de Gastos ao Longo do Tempo (AGORA USA O PERÍODO SELECIONADO)
        query_gastos_tempo_chart = sql.SQL(
            "SELECT dias.dia::date, COALESCE(SUM(g.valor), 0) as total_gasto "
            "FROM generate_series(%s::date, %s::date, '1 day'::interval) AS dias(dia) "
            "LEFT JOIN {schema}.gastos g ON dias.dia = g.data " # APENAS GASTOS VARIÁVEIS
            "GROUP BY dias.dia ORDER BY dias.dia ASC"
        ).format(schema=sql.Identifier(user_schema))
        cur.execute(query_gastos_tempo_chart, (data_inicio_periodo, data_fim_periodo))
        gastos_ultimos_dias_chart = cur.fetchall()
        dados['gastos_tempo_labels'] = [d_chart['dia'].strftime('%d/%m') for d_chart in gastos_ultimos_dias_chart]
        dados['gastos_tempo_data'] = [d_chart['total_gasto'] for d_chart in gastos_ultimos_dias_chart]

        # Calcular gastos fixos por dia para o mesmo período
        gastos_fixos_por_dia = {}
        for dia in dias_no_periodo_list:
            gastos_fixos_por_dia[dia] = Decimal('0.00')

        # Query para buscar gastos fixos ativos no período
        query_gastos_fixos_periodo = sql.SQL(
            "SELECT id, fecha_inicio, valor, recurrencia FROM {schema}.gastos_fixos "
            "WHERE activo = TRUE AND fecha_inicio <= %s"
        ).format(schema=sql.Identifier(user_schema))
        cur.execute(query_gastos_fixos_periodo, (data_fim_periodo,))
        gastos_fixos_periodo = cur.fetchall()

        for gf in gastos_fixos_periodo:
            rrule_params = get_rrule_params(gf['recurrencia'])
            if rrule_params:
                try:
                    dtstart_gf = gf['fecha_inicio']
                    if isinstance(dtstart_gf, datetime):
                        dtstart_gf = dtstart_gf.date()

                    occurrences = list(rrule(dtstart=dtstart_gf, until=data_fim_periodo, **rrule_params))
                    for occ_dt in occurrences:
                        occ_date = occ_dt.date()
                        if data_inicio_periodo <= occ_date <= data_fim_periodo:
                            gastos_fixos_por_dia[occ_date] += gf['valor']
                except Exception as e_rrule:
                    logging.error(f"Dashboard Gasto Fixo Tempo ID {gf['id']} rrule error: {e_rrule}")
            else:
                if gf['recurrencia'].lower().strip() in ['unico', 'único', 'única']:
                    if data_inicio_periodo <= gf['fecha_inicio'] <= data_fim_periodo:
                        gastos_fixos_por_dia[gf['fecha_inicio']] += gf['valor']

        # Converter dados de gastos fixos para a mesma ordem dos labels
        dados['gastos_fixos_tempo_data'] = []
        if dados.get('gastos_tempo_labels'):
            for label in dados['gastos_tempo_labels']:
                dia_str = label + '/' + str(data_inicio_periodo.year)
                try:
                    dia_obj = datetime.strptime(dia_str, '%d/%m/%Y').date()
                    dados['gastos_fixos_tempo_data'].append(gastos_fixos_por_dia.get(dia_obj, Decimal('0.00')))
                except ValueError:
                    dados['gastos_fixos_tempo_data'].append(Decimal('0.00'))

        # --- Buscar Metas Ativas (Não depende do período do card) ---
        metas_ativas = []
        meta_ativa = None
        try:
            query_metas_ativas = sql.SQL(
                "SELECT * FROM {schema}.metas WHERE status = 'ativa' ORDER BY criado_em DESC"
            ).format(schema=sql.Identifier(user_schema))
            cur.execute(query_metas_ativas)
            metas_ativas = cur.fetchall()
            
            # Para compatibilidade com o template existente, mantém meta_ativa como a primeira
            meta_ativa = metas_ativas[0] if metas_ativas else None
        except Exception as e:
            logging.error(f"Erro ao buscar metas ativas: {e}")
            metas_ativas = []
            meta_ativa = None

        # --- Buscar Gastos por Método de Pagamento para Gráfico (usando período selecionado) ---
        gastos_metodo_labels = []
        gastos_metodo_data = []
        try:
            query_gastos_metodo_chart = sql.SQL("""
                SELECT 
                    COALESCE(mp.nome, 'Sin Método Especificado') as metodo_nome,
                    SUM(g.valor) as total_gasto
                FROM {schema}.gastos g
                LEFT JOIN {schema}.metodos_pagamento mp ON g.metodo_pagamento_id = mp.id
                WHERE g.data BETWEEN %s AND %s
                GROUP BY mp.nome
                HAVING SUM(g.valor) > 0
                ORDER BY total_gasto DESC
            """).format(schema=sql.Identifier(user_schema))
            cur.execute(query_gastos_metodo_chart, (data_inicio_periodo, data_fim_periodo))
            gastos_por_metodo_chart = cur.fetchall()
            
            gastos_metodo_labels = [item['metodo_nome'] for item in gastos_por_metodo_chart]
            gastos_metodo_data = [item['total_gasto'] for item in gastos_por_metodo_chart]
            
            logging.info(f"Gastos por método de pagamento encontrados para período {periodo_selecionado}: {len(gastos_por_metodo_chart)} métodos")
        except Exception as e:
            logging.error(f"Error al buscar gastos por método de pagamento: {e}")
            gastos_metodo_labels = []
            gastos_metodo_data = []

        # --- Buscar Gastos Fijos Activos para el gráfico de próximos gastos ---
        try:
            query_gastos_fixos_proximos = sql.SQL(
                "SELECT id, fecha_inicio, descripcion, categoria, valor, recurrencia FROM {schema}.gastos_fixos "
                "WHERE activo = TRUE ORDER BY fecha_inicio ASC"
            ).format(schema=sql.Identifier(user_schema))
            cur.execute(query_gastos_fixos_proximos)
            gastos_fixos_raw = cur.fetchall()
            
            # Converter para lista de dicionários para serialização JSON
            gastos_fixos_ativos = []
            for gf in gastos_fixos_raw:
                gastos_fixos_ativos.append({
                    'id': gf['id'],
                    'fecha_inicio': gf['fecha_inicio'].isoformat() if gf['fecha_inicio'] else None,
                    'descripcion': gf['descripcion'],
                    'categoria': gf['categoria'],
                    'valor': float(gf['valor']) if gf['valor'] else 0,
                    'recurrencia': gf['recurrencia']
                })
            
            logging.info(f"Gastos fijos activos encontrados: {len(gastos_fixos_ativos)}")
        except Exception as e:
            logging.error(f"Error al buscar gastos fijos activos: {e}")
            gastos_fixos_ativos = []

        logging.info(f"Dashboard data calculated for schema {user_schema}. Meta ativa: {'Sim' if meta_ativa else 'Não'}")

    except psycopg2.Error as e:
        logging.error(f"Erro DB ao carregar dashboard para schema {user_schema}, período {periodo_selecionado}: {e}")
        flash('Erro ao buscar dados para o dashboard.', 'danger')
        meta_ativa = None
    except Exception as e:
        logging.error(f"Erro inesperado ao carregar dashboard para schema {user_schema}, período {periodo_selecionado}: {e}", exc_info=True)
        flash('Ocorreu um erro inesperado ao carregar o dashboard.', 'danger')
        meta_ativa = None
    finally:
        if cur: cur.close()
        if conn: conn.close()

    dados_json_string = json.dumps({
        "gastos_categoria_labels": dados['gastos_categoria_labels'],
        "gastos_categoria_data": dados['gastos_categoria_data'],
        "gastos_fixos_categoria_labels": dados['gastos_fixos_categoria_labels'],
        "gastos_fixos_categoria_data": dados['gastos_fixos_categoria_data'],
        "gastos_tempo_labels": dados['gastos_tempo_labels'],
        "gastos_tempo_data": dados['gastos_tempo_data'],
        "gastos_fixos_tempo_data": dados.get('gastos_fixos_tempo_data', []),
        "gastos_metodo_labels": gastos_metodo_labels if 'gastos_metodo_labels' in locals() else [],
        "gastos_metodo_data": gastos_metodo_data if 'gastos_metodo_data' in locals() else []
    }, default=json_converter)

    return render_template('dashboard.html',
                           user_nome=user_nome,
                           dados=dados,
                           meta_ativa=meta_ativa,
                           metas_ativas=metas_ativas if 'metas_ativas' in locals() else [],
                           dados_json=dados_json_string,
                           categorias_por_tipo=categorias_por_tipo,
                           metodos_pagamento_disponiveis=metodos_pagamento_disponiveis,
                           gastos_fixos_ativos=gastos_fixos_ativos,
                           periodo_ativo=periodo_selecionado) # Passa o período ativo para o template




@app.route('/logout')
def logout():
    session.clear()
    flash('Cerraste sesión de tu cuenta.', 'info')
    return redirect(url_for('login'))

# --- Rotas de Gastos (Variáveis e Fixos) ---


# No seu app.py, substitua a função gastos() inteira por esta:

# No seu app.py, certifique-se que a sua função gastos() está assim:

# No seu app.py, substitua a função gastos() por esta:

# No seu app.py, substitua a função gastos() por esta versão mais limpa:

@app.route('/gastos', methods=['GET'])
def gastos():
    if 'user_assinatura_id' not in session:
        flash('Necesitas iniciar sesión para acceder a esta página.', 'warning')
        return redirect(url_for('login'))

    user_schema = session.get('user_schema')
    user_nome = session.get('user_nome', session.get('user_email'))
    if not user_schema:
        flash('Error interno: Información del usuario incompleta.', 'danger')
        session.clear(); return redirect(url_for('login'))

    # --- 1. Processamento de Filtros e Ordenação ---
    tipo_gasto_ativo = request.args.get('tipo', 'variaveis').lower()
    page = request.args.get('page', 1, type=int)
    sort_by = request.args.get('sort_by', 'fecha_desc')

    filtros_aplicados = {k: v for k, v in request.args.items() if k != 'page'}
    if 'tipo' not in filtros_aplicados: filtros_aplicados['tipo'] = tipo_gasto_ativo
    if 'sort_by' not in filtros_aplicados: filtros_aplicados['sort_by'] = sort_by

    where_clauses, params = [], []
    today = date.today()
    main_alias = sql.Identifier('g')
    date_column_name = 'fecha_inicio' if tipo_gasto_ativo == 'fixos' else 'data'
    date_column = sql.Identifier(date_column_name)
    
    data_inicio_str = request.args.get('data_inicio')
    data_fim_str = request.args.get('data_fim')
    if data_inicio_str and data_fim_str:
        data_inicio_obj = datetime.strptime(data_inicio_str, '%Y-%m-%d').date()
        data_fim_obj = datetime.strptime(data_fim_str, '%Y-%m-%d').date()
        where_clauses.append(sql.SQL("{alias}.{date_col} BETWEEN %s AND %s").format(alias=main_alias, date_col=date_column))
        params.extend([data_inicio_obj, data_fim_obj])
    elif tipo_gasto_ativo == 'variaveis':
        data_inicio_obj = today.replace(day=1)
        data_fim_obj = today
        where_clauses.append(sql.SQL("{alias}.{date_col} BETWEEN %s AND %s").format(alias=main_alias, date_col=date_column))
        params.extend([data_inicio_obj, data_fim_obj])
        if 'data_inicio' not in filtros_aplicados: filtros_aplicados['data_inicio'] = data_inicio_obj.strftime('%Y-%m-%d')
        if 'data_fim' not in filtros_aplicados: filtros_aplicados['data_fim'] = data_fim_obj.strftime('%Y-%m-%d')
    
    categoria_filtro = request.args.get('categoria_filtro', 'todas')
    if categoria_filtro != 'todas':
        where_clauses.append(sql.SQL("{alias}.categoria = %s").format(alias=main_alias))
        params.append(categoria_filtro)
    
    where_sql = sql.SQL(" WHERE ") + sql.SQL(" AND ").join(where_clauses) if where_clauses else sql.SQL("")
    
    order_options = {
        'fecha_desc': sql.SQL("{alias}.{date_col} DESC").format(alias=main_alias, date_col=date_column),
        'fecha_asc': sql.SQL("{alias}.{date_col} ASC").format(alias=main_alias, date_col=date_column),
        'valor_desc': sql.SQL("{alias}.valor DESC").format(alias=main_alias),
        'valor_asc': sql.SQL("{alias}.valor ASC").format(alias=main_alias),
        'descricao_asc': sql.SQL("{alias}.descripcion ASC").format(alias=main_alias)
    }
    order_by_clause = order_options.get(sort_by, order_options['fecha_desc'])

    # --- 2. Inicialização dos Dados ---
    lista_itens, stats_gastos = [], {'total': Decimal('0.00'), 'promedio_diario': Decimal('0.00'), 'top_categoria': 'N/A'}
    gastos_previstos_mes = []
    total_previsto_mes = Decimal('0.00')
    categorias_para_filtro, categorias_add_edit, metodos_pagamento = [], [], []
    total_items, total_pages, current_page = 0, 1, page

    conn = get_db_connection()
    if not conn:
        flash('Error de conexión con la base de datos.', 'danger'); return render_template('gastos.html', **locals())

    cur = None
    try:
        cur = conn.cursor(cursor_factory=DictCursor)
        table_name = sql.Identifier('gastos_fixos') if tipo_gasto_ativo == 'fixos' else sql.Identifier('gastos')
        
        cat_tipo = 'gasto_fixo' if tipo_gasto_ativo == 'fixos' else 'gasto_variavel'
        categorias_add_edit = buscar_categorias_por_tipo(conn, user_schema, cat_tipo)
        categorias_para_filtro = buscar_categorias_por_tipo(conn, user_schema, 'gasto_variavel') + buscar_categorias_por_tipo(conn, user_schema, 'gasto_fixo')
        metodos_pagamento = buscar_metodos_pagamento_ativos(conn, user_schema)

        if tipo_gasto_ativo == 'variaveis':
            select_sql = sql.SQL("SELECT {alias}.*, mp.nome as metodo_pagamento_nome FROM {schema}.{table} {alias} LEFT JOIN {schema}.metodos_pagamento mp ON {alias}.metodo_pagamento_id = mp.id").format(
                alias=main_alias, schema=sql.Identifier(user_schema), table=table_name)
        else: # fixos
            select_sql = sql.SQL("SELECT {alias}.*, NULL as metodo_pagamento_nome FROM {schema}.{table} {alias}").format(
                alias=main_alias, schema=sql.Identifier(user_schema), table=table_name)

        count_query = sql.SQL("SELECT COUNT(*) FROM {schema}.{table} {alias} {where}").format(schema=sql.Identifier(user_schema), table=table_name, alias=main_alias, where=where_sql)
        cur.execute(count_query, params)
        total_items = cur.fetchone()[0]
        total_pages = ceil(total_items / ITEMS_PER_PAGE) if total_items > 0 else 1
        current_page = min(page, total_pages) if total_pages > 0 else 1
        offset = (current_page - 1) * ITEMS_PER_PAGE
        
        main_query_sql = sql.SQL("{select} {where} ORDER BY {order} LIMIT %s OFFSET %s").format(select=select_sql, where=where_sql, order=order_by_clause)
        cur.execute(main_query_sql, params + [ITEMS_PER_PAGE, offset])
        lista_itens = cur.fetchall()

        if where_clauses: 
            stats_query = sql.SQL("SELECT COALESCE(SUM({alias}.valor), 0) as total, (SELECT categoria FROM {schema}.{table} {alias} {where} GROUP BY {alias}.categoria ORDER BY SUM({alias}.valor) DESC LIMIT 1) as top_cat FROM {schema}.{table} {alias} {where}").format(alias=main_alias, schema=sql.Identifier(user_schema), table=table_name, where=where_sql)
            cur.execute(stats_query, params * 2)
            stats_result = cur.fetchone();
            if stats_result:
                stats_gastos['total'] = stats_result['total']
                if 'data_inicio' in filtros_aplicados and 'data_fim' in filtros_aplicados:
                    dias_periodo = (datetime.strptime(filtros_aplicados['data_fim'], '%Y-%m-%d').date() - datetime.strptime(filtros_aplicados['data_inicio'], '%Y-%m-%d').date()).days + 1
                    stats_gastos['promedio_diario'] = stats_result['total'] / dias_periodo if dias_periodo > 0 else Decimal('0.00')
                stats_gastos['top_categoria'] = stats_result['top_cat'] or 'N/A'
        
        if tipo_gasto_ativo == 'fixos':
            query_all_fixos = sql.SQL("SELECT * FROM {schema}.gastos_fixos WHERE activo = TRUE").format(schema=sql.Identifier(user_schema))
            cur.execute(query_all_fixos)
            todos_gastos_fixos = cur.fetchall()
            primeiro_dia_mes_atual = today.replace(day=1)
            ultimo_dia_mes_atual = (primeiro_dia_mes_atual + relativedelta(months=1)) - timedelta(days=1)
            for gf in todos_gastos_fixos:
                dia_vencimento = gf['fecha_inicio'].day
                try: vencimento_neste_mes = today.replace(day=dia_vencimento)
                except ValueError: 
                    ultimo_dia_mes = (today.replace(day=1) + relativedelta(months=1)) - timedelta(days=1)
                    vencimento_neste_mes = ultimo_dia_mes
                proximo_vencimento = vencimento_neste_mes
                if vencimento_neste_mes < today: proximo_vencimento += relativedelta(months=1)
                if primeiro_dia_mes_atual <= proximo_vencimento <= ultimo_dia_mes_atual:
                    gasto_previsto = dict(gf); gasto_previsto['data_vencimento'] = proximo_vencimento
                    gastos_previstos_mes.append(gasto_previsto); total_previsto_mes += gf['valor']
            gastos_previstos_mes.sort(key=lambda x: x['data_vencimento'])
            # A variável 'proximo_gasto_a_vencer' foi removida daqui

    except Exception as e:
        flash('Ocurrió un error al procesar los datos de gastos.', 'danger'); logging.error(f"Error al procesar gastos para {user_schema}: {e}", exc_info=True)
    finally:
        if cur: cur.close()
        if conn: conn.close()

    # Passando as variáveis para o template, agora sem 'proximo_gasto_a_vencer'
    return render_template('gastos.html',
                           user_nome=user_nome,
                           itens=lista_itens,
                           stats_gastos=stats_gastos,
                           categorias_para_filtro=sorted(list(set(categorias_para_filtro))),
                           categorias_disponiveis_add_edit=categorias_add_edit,
                           metodos_pagamento_disponiveis=metodos_pagamento,
                           filtros_aplicados=filtros_aplicados,
                           tipo_gasto_ativo=tipo_gasto_ativo,
                           current_page=current_page,
                           total_pages=total_pages,
                           gastos_previstos_mes=gastos_previstos_mes,
                           total_previsto_mes=total_previsto_mes)

    
@app.route('/add_gasto', methods=['POST'])
def add_gasto():
    # Verifica sessão
    if 'user_assinatura_id' not in session:
        flash('Sessão expirada.', 'warning')
        return redirect(url_for('login'))

    user_schema = session.get('user_schema')
    # Define URL de redirecionamento (pode vir do dashboard ou da página de gastos)
    redirect_url = request.referrer or url_for('gastos', tipo='variaveis')

    # Verifica schema
    if not user_schema:
        flash('Erro interno.', 'danger')
        session.clear()
        return redirect(url_for('login'))

    # Obtém dados do formulário
    descricao_form = request.form.get('descricao')
    valor_str = request.form.get('valor')
    categoria = request.form.get('categoria')
    data_gasto_str = request.form.get('data')
    metodo_pagamento_id = request.form.get('metodo_pagamento')

    # Validação básica dos campos
    if not all([descricao_form, valor_str, categoria, data_gasto_str]):
        flash('Campos obrigatórios: Descrição, Valor, Categoria e Data.', 'danger')
        return redirect(redirect_url)

    # Validação do valor
    try:
        valor_decimal = Decimal(valor_str.replace(',', '.'))
        if valor_decimal <= 0:
             raise ValueError("O valor do gasto deve ser positivo.")
    except (InvalidOperation, ValueError) as e:
        flash(f'Valor inválido: {e}', 'danger')
        return redirect(redirect_url)

    # Validação da data
    try:
        data_gasto_obj = datetime.strptime(data_gasto_str, '%Y-%m-%d').date()
    except ValueError:
        flash('Data inválida. Use o formato AAAA-MM-DD.', 'danger')
        return redirect(redirect_url)

    # Conexão com o banco
    conn = get_db_connection()
    if not conn:
        flash('Erro de conexão com o banco de dados.', 'danger')
        return redirect(redirect_url)

    cur = None
    try:
        # *** Validação da Categoria ***
        if not validar_categoria(conn, user_schema, categoria, 'gasto_variavel'):
            flash(f'Erro: Categoria "{categoria}" inválida ou não permitida para Gasto Variável.', 'danger')
            logging.warning(f"Tentativa de adicionar gasto variável com categoria inválida '{categoria}' no schema {user_schema}")
            # Não precisa fechar conn/cur aqui, o finally cuida disso
            return redirect(redirect_url)

        # Validação do método de pagamento (opcional)
        metodo_pagamento_id_final = None
        if metodo_pagamento_id and metodo_pagamento_id.isdigit():
            # Verifica se o método de pagamento existe e está ativo
            cur_temp = conn.cursor()
            check_metodo_query = sql.SQL("SELECT id FROM {schema}.metodos_pagamento WHERE id = %s AND ativo = TRUE").format(
                schema=sql.Identifier(user_schema)
            )
            cur_temp.execute(check_metodo_query, (int(metodo_pagamento_id),))
            if cur_temp.fetchone():
                metodo_pagamento_id_final = int(metodo_pagamento_id)
            cur_temp.close()

        # Se a categoria é válida, prossegue com a inserção
        cur = conn.cursor()
        insert_query = sql.SQL("INSERT INTO {}.gastos (descripcion, valor, categoria, data, metodo_pagamento_id) VALUES (%s, %s, %s, %s, %s)").format(
            sql.Identifier(user_schema)
        )
        cur.execute(insert_query, (descricao_form, valor_decimal, categoria, data_gasto_obj, metodo_pagamento_id_final))
        conn.commit()
        flash('¡Gasto variable agregado con éxito!', 'success')
        logging.info(f"Gasto variável '{descricao_form}' adicionado para schema {user_schema}")

    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash(f'Erro no banco de dados ao adicionar gasto: {e}', 'danger')
        logging.error(f"Erro DB add gasto var {user_schema}: {e}")
    except Exception as e:
        if conn: conn.rollback()
        flash(f'Erro inesperado ao adicionar gasto: {e}', 'danger')
        logging.error(f"Erro inesperado add gasto var {user_schema}: {e}", exc_info=True)
    finally:
        # Garante que cursor e conexão sejam fechados
        if cur: cur.close()
        if conn: conn.close()

    return redirect(redirect_url)

@app.route('/add_gasto_fixo', methods=['POST'])
def add_gasto_fixo():
    # Verifica sessão
    if 'user_assinatura_id' not in session:
        flash('Sessão expirada.', 'warning')
        return redirect(url_for('login'))

    user_schema = session.get('user_schema')
    # Define URL de redirecionamento
    redirect_url = request.referrer or url_for('gastos', tipo='fixos')

    # Verifica schema
    if not user_schema:
        flash('Erro interno.', 'danger')
        session.clear()
        return redirect(url_for('login'))

    # --- INÍCIO DA CORREÇÃO: Usar os nomes unificados do formulário ---
    descricao = request.form.get('descricao')
    valor_str = request.form.get('valor')
    categoria = request.form.get('categoria')
    fecha_inicio_str = request.form.get('fecha_inicio_fixo')
    # --- FIM DA CORREÇÃO ---

    recurrencia = 'mensual'  # Padrão para gastos fixos
    activo = True

    # Validação básica dos campos
    if not all([descricao, valor_str, categoria, fecha_inicio_str]):
        flash('Campos obrigatórios: Descrição, Valor, Categoria e Data Início.', 'danger')
        return redirect(redirect_url)

    # Validação do valor
    try:
        valor_decimal = Decimal(valor_str.replace(',', '.'))
        if valor_decimal <= 0:
            raise ValueError("O valor do gasto fixo deve ser positivo.")
    except (InvalidOperation, ValueError) as e:
        flash(f'Valor inválido: {e}', 'danger')
        return redirect(redirect_url)

    # Validação da data
    try:
        fecha_inicio_obj = datetime.strptime(fecha_inicio_str, '%Y-%m-%d').date()
    except ValueError:
        flash('Data de início inválida. Use o formato AAAA-MM-DD.', 'danger')
        return redirect(redirect_url)
        
    conn = get_db_connection()
    if not conn:
        flash('Erro de conexão com o banco de dados.', 'danger')
        return redirect(redirect_url)

    cur = None
    try:
        # Validação da Categoria
        if not validar_categoria(conn, user_schema, categoria, 'gasto_fixo'):
            flash(f'Erro: Categoria "{categoria}" inválida ou não permitida para Gasto Fixo.', 'danger')
            logging.warning(f"Tentativa de adicionar gasto fixo com categoria inválida '{categoria}' no schema {user_schema}")
            return redirect(redirect_url)

        cur = conn.cursor()
        insert_query = sql.SQL("""
            INSERT INTO {schema}.gastos_fixos
            (descripcion, valor, categoria, fecha_inicio, recurrencia, activo)
            VALUES (%s, %s, %s, %s, %s, %s)
        """).format(schema=sql.Identifier(user_schema))
        
        cur.execute(insert_query, (descricao, valor_decimal, categoria, fecha_inicio_obj, recurrencia, activo))
        
        conn.commit()
        flash('¡Gasto fijo agregado con éxito!', 'success')
        logging.info(f"Gasto fixo '{descricao}' adicionado para schema {user_schema}")

    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash(f'Error: {e}', 'danger')
        logging.error(f"Erro DB add gasto fixo {user_schema}: {e}")
    except Exception as e:
        if conn: conn.rollback()
        flash(f'Error: {e}', 'danger')
        logging.error(f"Erro inesperado add gasto fixo {user_schema}: {e}", exc_info=True)
    finally:
        if cur: cur.close()
        if conn: conn.close()

    return redirect(redirect_url)

# No seu app.py, substitua a função lembretes() inteira por esta:

@app.route('/lembretes')
def lembretes():
    if 'user_assinatura_id' not in session:
        flash('Login necessário.', 'warning')
        return redirect(url_for('login'))
    
    user_schema = session.get('user_schema')
    user_nome = session.get('user_nome', session.get('user_email'))
    if not user_schema:
        flash('Erro interno.', 'danger')
        session.clear()
        return redirect(url_for('login'))

    conn = get_db_connection()
    if not conn:
        flash('Erro conexão DB.', 'danger')
        return render_template('lembretes.html', user_nome=user_nome, lembretes_agrupados={})

    lista_de_lembretes = []
    cur = None
    try:
        cur = conn.cursor(cursor_factory=DictCursor)
        query = sql.SQL("""
            SELECT id, descripcion, data, valor, repetir, tipo_repeticion 
            FROM {schema}.lembretes ORDER BY data ASC, id ASC
        """).format(schema=sql.Identifier(user_schema))
        cur.execute(query)
        lista_de_lembretes = cur.fetchall()
    except psycopg2.Error as e:
        flash('Erro DB ao buscar lembretes.', 'danger')
        logging.error(f"Erro DB /lembretes {user_schema}: {e}")
    finally:
        if cur: cur.close()
        if conn: conn.close()

    # --- Lógica de Agrupamento Inteligente ---
    hoje = date.today()
    limite_proximos_dias = hoje + timedelta(days=7)
    
    lembretes_agrupados = {
        'vencidos': [],
        'para_hoje': [],
        'proximos_7_dias': [],
        'futuros': []
    }

    for lembrete in lista_de_lembretes:
        lembrete_dict = dict(lembrete) # Converte para um dicionário mutável
        data_lembrete = lembrete_dict['data']
        is_repeating_monthly = lembrete_dict.get('repetir') and lembrete_dict.get('tipo_repeticion') == 'mensal'

        if is_repeating_monthly:
            # Calcula a próxima ocorrência a partir de hoje
            dia_lembrete = data_lembrete.day
            try:
                proxima_ocorrencia = hoje.replace(day=dia_lembrete)
            except ValueError: # Caso o dia não exista no mês atual (ex: dia 31 em fevereiro)
                ultimo_dia_mes_atual = (hoje.replace(day=1) + relativedelta(months=1)) - timedelta(days=1)
                proxima_ocorrencia = ultimo_dia_mes_atual
            
            if proxima_ocorrencia < hoje:
                proxima_ocorrencia = proxima_ocorrencia + relativedelta(months=1)

            lembrete_dict['data_exibicao'] = proxima_ocorrencia # Adiciona data para exibição
            
            # Agrupa baseado na próxima ocorrência
            if proxima_ocorrencia == hoje:
                lembretes_agrupados['para_hoje'].append(lembrete_dict)
            elif hoje < proxima_ocorrencia <= limite_proximos_dias:
                lembretes_agrupados['proximos_7_dias'].append(lembrete_dict)
            else:
                lembretes_agrupados['futuros'].append(lembrete_dict)
        
        else: # Lembretes não repetitivos
            lembrete_dict['data_exibicao'] = data_lembrete
            if data_lembrete < hoje:
                lembretes_agrupados['vencidos'].append(lembrete_dict)
            elif data_lembrete == hoje:
                lembretes_agrupados['para_hoje'].append(lembrete_dict)
            elif hoje < data_lembrete <= limite_proximos_dias:
                lembretes_agrupados['proximos_7_dias'].append(lembrete_dict)
            else: # data_lembrete > limite_proximos_dias
                lembretes_agrupados['futuros'].append(lembrete_dict)
    
    # Ordena cada grupo pela data de exibição
    for grupo in lembretes_agrupados.values():
        grupo.sort(key=lambda x: x['data_exibicao'])

    return render_template('lembretes.html', 
                           user_nome=user_nome, 
                           lembretes_agrupados=lembretes_agrupados)

@app.route('/add_lembrete_from_modal', methods=['POST'])
def add_lembrete_from_modal():
    # 1. Verifica sessão
    if 'user_assinatura_id' not in session:
        flash('Sessão expirada.', 'warning')
        return redirect(url_for('login'))

    # 2. Obtém schema e URL de redirecionamento
    user_schema = session.get('user_schema')
    referer_url = request.referrer
    # Decide se volta para lembretes ou dashboard
    redirect_url = url_for('lembretes') if referer_url and '/lembretes' in referer_url else url_for('dashboard')

    if not user_schema:
        flash('Erro interno.', 'danger')
        session.clear()
        return redirect(url_for('login'))

    # --- 3. Obter e Validar Dados do Formulário ---
    descricao = request.form.get('descricao_lembrete')
    data_lembrete_str = request.form.get('data_lembrete')
    valor_str = request.form.get('valor_lembrete', '').strip()

    # Validação básica
    if not descricao or not data_lembrete_str:
        flash('Descrição e data obrigatórias.', 'danger')
        return redirect(redirect_url)

    # --- Tratamento do Valor ---
    valor_decimal = Decimal('0.00')  # Valor padrão
    if valor_str:
        try:
            valor_decimal = Decimal(valor_str.replace(',', '.'))
            if valor_decimal < 0:
                raise ValueError("O valor deve ser positivo ou zero.")
        except (InvalidOperation, ValueError) as e:
            flash(f'Valor inválido: {e}', 'danger')
            return redirect(redirect_url)

    # Validação da data
    try:
        data_lembrete_obj = datetime.strptime(data_lembrete_str, '%Y-%m-%d').date()
    except ValueError:
        flash('Data inválida. Use AAAA-MM-DD.', 'danger')
        return redirect(redirect_url)

    # --- 4. Tratamento da Repetição (Correção Principal) ---
    repetir_str = request.form.get('repetir_lembrete', 'false') # Pega do select 'Sim'/'Não'
    repetir = repetir_str.lower() == 'true' # Converte para booleano
    tipo_rep = None # Inicializa como None

    if repetir:
        # Pega o valor do select de tipo de repetição (que só aparece se 'Sim' for selecionado)
        tipo_rep_form = request.form.get('tipo_repeticion_lembrete')
        # Valida se é 'mensal' (única opção suportada no modal do dashboard atualmente)
        if tipo_rep_form and tipo_rep_form.lower() == 'mensal':
            tipo_rep = 'mensal'
        else:
            # Se 'repetir' for true mas o tipo for inválido ou ausente, assume 'mensal'
            tipo_rep = 'mensal'
            logging.warning(
                f"Tipo de repetição inválido/ausente ('{tipo_rep_form}') ao adicionar lembrete via modal. "
                f"Assumindo 'mensal'. Schema: {user_schema}"
            )
    # Se 'repetir' for False, tipo_rep continua None

    # --- 5. Conexão e Inserção no Banco ---
    conn = get_db_connection()
    if not conn:
        flash('Erro conexão DB.', 'danger')
        return redirect(redirect_url)

    cur = None
    try:
        cur = conn.cursor()
        # Query INSERT atualizada para incluir as colunas valor, repetir e tipo_repeticion
        query = sql.SQL("""
            INSERT INTO {schema}.lembretes (descripcion, data, valor, repetir, tipo_repeticion)
            VALUES (%s, %s, %s, %s, %s)
        """).format(schema=sql.Identifier(user_schema))
        # Parâmetros agora incluem valor, repetição e tipo
        params = (descricao, data_lembrete_obj, valor_decimal, repetir, tipo_rep)

        logging.debug(f"Executando INSERT Lembrete (Modal): Query={query.as_string(conn)} Params={params}")
        cur.execute(query, params)
        conn.commit()
        flash('¡Recordatorio agregado con éxito!', 'success')
        logging.info(f"Lembrete '{descricao}' adicionado via modal (repetir={repetir}, tipo={tipo_rep}). Schema: {user_schema}")

    except psycopg2.Error as e:
        if conn: conn.rollback()
        logging.error(f"Erro DB add lembrete (modal) {user_schema}: {e}")
        flash(f'Erro DB: {e}', 'danger')
    except Exception as e:
        if conn: conn.rollback()
        logging.error(f"Erro inesperado add lembrete (modal) {user_schema}: {e}", exc_info=True)
        flash('Erro inesperado.', 'danger')
    finally:
        if cur: cur.close()
        if conn: conn.close()

    # 6. Redireciona para a página correta
    return redirect(redirect_url)

# --- Rotas de Receitas ---


# No seu app.py, substitua a função receitas() inteira por esta:

@app.route('/receitas', methods=['GET'])
def receitas():
    if 'user_assinatura_id' not in session:
        flash('Você precisa fazer login para acessar esta página.', 'warning')
        return redirect(url_for('login'))

    user_schema = session.get('user_schema')
    user_nome = session.get('user_nome', session.get('user_email'))
    if not user_schema:
        flash('Erro interno: Informações do usuário incompletas.', 'danger')
        session.clear()
        return redirect(url_for('login'))

    # --- 1. Processamento de Filtros ---
    today = date.today()
    # Define o primeiro dia do mês atual como padrão se não houver filtro
    default_start_date = today.replace(day=1)
    
    data_inicio_str = request.args.get('data_inicio')
    data_fim_str = request.args.get('data_fim')
    categoria_filtro = request.args.get('categoria_filtro', 'todas')
    sort_by = request.args.get('sort_by', 'fecha_desc')
    page = request.args.get('page', 1, type=int)

    # Define o período do filtro: se não houver filtro, usa o mês atual.
    data_inicio_obj = datetime.strptime(data_inicio_str, '%Y-%m-%d').date() if data_inicio_str else default_start_date
    data_fim_obj = datetime.strptime(data_fim_str, '%Y-%m-%d').date() if data_fim_str else today

    filtros_aplicados = {
        'data_inicio': data_inicio_str, 'data_fim': data_fim_str,
        'categoria_filtro': categoria_filtro, 'sort_by': sort_by
    }

    # --- 2. Inicialização dos Dados ---
    lista_receitas = []
    stats_receitas = {
        'total': Decimal('0.00'),
        'promedio': Decimal('0.00'),
        'categoria_principal': 'N/A'
    }
    categorias_disponiveis = []
    categorias_receitas_formulario = []
    total_items = 0
    total_pages = 1

    conn = get_db_connection()
    if not conn:
        flash('Erro de conexão com o banco de dados.', 'danger')
        return render_template('receitas.html', user_nome=user_nome, receitas=[], stats_receitas=stats_receitas, categorias_disponiveis=[], categorias_receitas_formulario=[], filtros_aplicados=filtros_aplicados, current_page=1, total_pages=1)

    cur = None
    try:
        cur = conn.cursor(cursor_factory=DictCursor)

        # --- 3. Construir Query com Filtros ---
        where_clauses = [sql.SQL("fecha BETWEEN %s AND %s")]
        query_params = [data_inicio_obj, data_fim_obj]
        
        if categoria_filtro != 'todas':
            where_clauses.append(sql.SQL("categoria = %s"))
            query_params.append(categoria_filtro)
        
        where_sql = sql.SQL(" WHERE ") + sql.SQL(" AND ").join(where_clauses)

        # --- 4. Calcular Estatísticas para o Mini-Dashboard ---
        stats_query = sql.SQL("""
            SELECT 
                COALESCE(SUM(valor), 0) as total,
                COALESCE(AVG(valor), 0) as promedio,
                (SELECT categoria FROM {schema}.outras_receitas {where} GROUP BY categoria ORDER BY SUM(valor) DESC LIMIT 1) as categoria_principal
            FROM {schema}.outras_receitas {where}
        """).format(schema=sql.Identifier(user_schema), where=where_sql)
        cur.execute(stats_query, query_params * 2) # Parâmetros são necessários para a subquery também
        stats_result = cur.fetchone()
        if stats_result:
            stats_receitas['total'] = stats_result['total']
            stats_receitas['promedio'] = stats_result['promedio']
            stats_receitas['categoria_principal'] = stats_result['categoria_principal'] or 'N/A'

        # --- 5. Buscar Lista Paginada de Transações ---
        count_query = sql.SQL("SELECT COUNT(*) FROM {schema}.outras_receitas {where}").format(schema=sql.Identifier(user_schema), where=where_sql)
        cur.execute(count_query, query_params)
        total_items = cur.fetchone()[0]
        total_pages = ceil(total_items / ITEMS_PER_PAGE) if total_items > 0 else 1
        current_page = min(page, total_pages) if total_pages > 0 else 1
        offset = (current_page - 1) * ITEMS_PER_PAGE

        order_by_options = {
            'fecha_desc': sql.SQL("ORDER BY fecha DESC, id DESC"), 'fecha_asc': sql.SQL("ORDER BY fecha ASC, id ASC"),
            'valor_desc': sql.SQL("ORDER BY valor DESC, fecha DESC"), 'valor_asc': sql.SQL("ORDER BY valor ASC, fecha DESC"),
            'categoria_asc': sql.SQL("ORDER BY categoria ASC, fecha DESC")
        }
        order_by_clause = order_by_options.get(sort_by, order_by_options['fecha_desc'])
        
        main_query = sql.SQL("SELECT id, fecha, categoria, descripcion, valor FROM {schema}.outras_receitas {where} {order} LIMIT %s OFFSET %s").format(
            schema=sql.Identifier(user_schema), where=where_sql, order=order_by_clause)
        cur.execute(main_query, query_params + [ITEMS_PER_PAGE, offset])
        lista_receitas = cur.fetchall()

        # --- 6. Buscar Categorias para os Filtros ---
        categorias_disponiveis = buscar_categorias_por_tipo(conn, user_schema, 'receita')
        categorias_receitas_formulario = categorias_disponiveis # Reutiliza a mesma lista

    except psycopg2.Error as e:
        flash('Erro de banco de dados na página de receitas.', 'danger')
        logging.error(f"Erro DB /receitas {user_schema}: {e}")
    finally:
        if cur: cur.close()
        if conn: conn.close()

    return render_template('receitas.html',
                           user_nome=user_nome,
                           receitas=lista_receitas,
                           stats_receitas=stats_receitas,
                           categorias_disponiveis=categorias_disponiveis,
                           categorias_receitas_formulario=categorias_receitas_formulario,
                           filtros_aplicados=filtros_aplicados,
                           current_page=current_page,
                           total_pages=total_pages)




@app.route('/add_outra_receita', methods=['POST'])
def add_outra_receita():
    # Verifica sessão
    if 'user_assinatura_id' not in session:
        flash('Sessão expirada.', 'warning')
        return redirect(url_for('login'))

    user_schema = session.get('user_schema')
    # Define URL de redirecionamento
    redirect_url = request.referrer or url_for('receitas')

    # Verifica schema
    if not user_schema:
        flash('Erro interno.', 'danger')
        session.clear()
        return redirect(url_for('login'))

    # Obtém dados do formulário
    data_receita_str = request.form.get('data_outra_receita')
    categoria_receita = request.form.get('categoria_outra_receita')
    descricao_receita = request.form.get('descricao_outra_receita')
    valor_receita_str = request.form.get('valor_outra_receita')

    # Validação básica dos campos
    if not all([data_receita_str, categoria_receita, descricao_receita, valor_receita_str]):
        flash('Campos obrigatórios: Data, Categoria, Descrição e Valor.', 'danger')
        return redirect(redirect_url)

    # Validação do valor
    try:
        valor_receita_decimal = Decimal(valor_receita_str.replace(',', '.'))
        if valor_receita_decimal <= 0:
            raise ValueError("O valor da receita deve ser positivo.")
    except (InvalidOperation, ValueError) as e:
        flash(f'Valor inválido: {e}', 'danger')
        return redirect(redirect_url)

    # Validação da data
    try:
        data_receita_obj = datetime.strptime(data_receita_str, '%Y-%m-%d').date()
    except ValueError:
        flash('Data inválida. Use o formato AAAA-MM-DD.', 'danger')
        return redirect(redirect_url)

    # Conexão com o banco
    conn = get_db_connection()
    if not conn:
        flash('Erro de conexão com o banco de dados.', 'danger')
        return redirect(redirect_url)

    cur = None
    try:
        # *** Validação da Categoria ***
        if not validar_categoria(conn, user_schema, categoria_receita, 'receita'):
            flash(f'Erro: Categoria "{categoria_receita}" inválida ou não permitida para Receita.', 'danger')
            logging.warning(f"Tentativa de adicionar outra receita com categoria inválida '{categoria_receita}' no schema {user_schema}")
            return redirect(redirect_url)

        # Se a categoria é válida, prossegue com a inserção
        cur = conn.cursor()
        insert_query = sql.SQL("""
            INSERT INTO {schema}.outras_receitas
            (fecha, categoria, descripcion, valor)
            VALUES (%s, %s, %s, %s)
        """).format(schema=sql.Identifier(user_schema))
        cur.execute(insert_query, (data_receita_obj, categoria_receita, descricao_receita, valor_receita_decimal))
        conn.commit()
        flash('¡Ingreso agregado con éxito!', 'success')
        logging.info(f"Outra receita '{descricao_receita}' adicionada para schema {user_schema}")

    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash(f'Error {e}', 'danger')
        logging.error(f"Erro DB add outra receita {user_schema}: {e}")
    except Exception as e:
        if conn: conn.rollback()
        flash(f'Error: {e}', 'danger')
        logging.error(f"Erro inesperado add outra receita {user_schema}: {e}", exc_info=True)
    finally:
        if cur: cur.close()
        if conn: conn.close()

    return redirect(redirect_url)


# --- Rotas de Metas ---
@app.route('/metas', methods=['GET', 'POST'])
def metas():
    if 'user_assinatura_id' not in session:
        flash('Necesitas iniciar sesión para acceder a esta página.', 'warning')
        return redirect(url_for('login'))

    user_schema = session.get('user_schema')
    user_nome = session.get('user_nome', session.get('user_email'))
    if not user_schema:
        flash('Error interno: Schema del usuario no encontrado.', 'danger')
        session.clear()
        return redirect(url_for('login'))

    if request.method == 'POST':
        # Procesamiento del formulario de creación/edición de meta
        conn = get_db_connection()
        if not conn:
            flash('Error de conexión con la base de datos.', 'danger')
            return redirect(url_for('metas'))

        cur = None
        try:
            descricao = request.form.get('meta_descricao')
            categoria = request.form.get('meta_categoria')
            prazo_form_value = request.form.get('meta_prazo')
            valor_alvo_str = request.form.get('meta_valor_alvo')
            meta_id = request.form.get('meta_id')  # Para edición

            if not all([descricao, categoria, prazo_form_value, valor_alvo_str]):
                flash('Error: Todos los campos son obligatorios.', 'danger')
                return redirect(url_for('metas'))

            try:
                valor_alvo = Decimal(valor_alvo_str.replace(',', '.'))
                if valor_alvo <= 0:
                    raise ValueError("El valor objetivo de la meta debe ser un número positivo.")
            except (InvalidOperation, ValueError) as e:
                flash(f'Error: Valor objetivo inválido. {e}', 'danger')
                return redirect(url_for('metas'))

            prazo_meses = None
            if prazo_form_value and prazo_form_value.isdigit():
                try:
                    prazo_meses = int(prazo_form_value)
                    if prazo_meses <= 0:
                        flash('Error: El plazo en meses debe ser un número positivo.', 'danger')
                        return redirect(url_for('metas'))
                except ValueError:
                    flash('Error: Plazo en meses inválido.', 'danger')
                    return redirect(url_for('metas'))
            elif prazo_form_value != 'indefinido':
                flash('Error: Valor de plazo inválido.', 'danger')
                return redirect(url_for('metas'))

            valid_categorias = ['Viaje', 'Compra', 'Ahorrar dinero', 'Otros']
            if categoria not in valid_categorias:
                flash('Error: Categoría de meta inválida.', 'danger')
                return redirect(url_for('metas'))

            valor_mensal_sugerido = None
            data_conclusao_prevista = None
            data_inicio = date.today()
            
            if prazo_meses and valor_alvo > 0:
                try:
                    valor_mensal_sugerido = round(valor_alvo / Decimal(prazo_meses), 2)
                    data_conclusao_prevista = data_inicio + relativedelta(months=prazo_meses)
                except Exception as e:
                    logging.error(f"Error cálculo meta {user_schema}: {e}")

            cur = conn.cursor()
            
            if meta_id and meta_id.isdigit():
                # Edición de meta existente
                update_query = sql.SQL("""
                    UPDATE {schema}.metas 
                    SET descricao = %s, categoria = %s, prazo_meses = %s, valor_alvo = %s, 
                        valor_mensal_sugerido = %s, data_conclusao_prevista = %s, atualizado_em = NOW()
                    WHERE id = %s AND status = 'ativa'
                """).format(schema=sql.Identifier(user_schema))
                cur.execute(update_query, (descricao, categoria, prazo_meses, valor_alvo, valor_mensal_sugerido, data_conclusao_prevista, int(meta_id)))
                if cur.rowcount > 0:
                    flash('¡Meta actualizada con éxito!', 'success')
                else:
                    flash('Meta no encontrada o no se pudo actualizar.', 'warning')
            else:
                # Creación de nueva meta
                insert_query = sql.SQL("""
                    INSERT INTO {schema}.metas 
                    (descricao, categoria, prazo_meses, valor_alvo, valor_mensal_sugerido, 
                     data_inicio, data_conclusao_prevista, valor_atual, status) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 0, 'ativa')
                """).format(schema=sql.Identifier(user_schema))
                cur.execute(insert_query, (descricao, categoria, prazo_meses, valor_alvo, valor_mensal_sugerido, data_inicio, data_conclusao_prevista))
                flash('¡Nueva meta creada con éxito!', 'success')
            
            conn.commit()

        except psycopg2.Error as e:
            conn.rollback()
            flash(f'Error en la base de datos: {e}', 'danger')
            logging.error(f"Error DB guardar meta {user_schema}: {e}")
        except Exception as e:
            conn.rollback()
            flash(f'Error inesperado: {e}', 'danger')
            logging.error(f"Error inesperado guardar meta {user_schema}: {e}")
        finally:
            if cur: cur.close()
            if conn: conn.close()

        return redirect(url_for('metas'))

    # GET - Mostrar página de metas
    lista_metas = []
    conn = get_db_connection()
    if conn:
        cur = None
        try:
            cur = conn.cursor(cursor_factory=DictCursor)
            query_metas = sql.SQL("""
                SELECT * FROM {schema}.metas 
                WHERE status IN ('ativa', 'concluida') 
                ORDER BY 
                    CASE WHEN status = 'ativa' THEN 1 ELSE 2 END,
                    criado_em DESC
            """).format(schema=sql.Identifier(user_schema))
            cur.execute(query_metas)
            lista_metas = cur.fetchall()
            logging.info(f"Metas consultadas para schema {user_schema}: {len(lista_metas)} encontradas.")
        except psycopg2.Error as e:
            flash('Error al consultar metas en la base de datos.', 'danger')
            logging.error(f"Error DB al consultar metas (schema {user_schema}): {e}")
        finally:
            if cur: cur.close()
            if conn: conn.close()
    else:
        flash('Error de conexión con la base de datos.', 'danger')

    return render_template('metas.html', user_nome=user_nome, metas=lista_metas)

@app.route('/metas/<int:meta_id>/progresso', methods=['POST'])
def add_progresso_meta(meta_id):
    if 'user_assinatura_id' not in session:
        flash('Sesión expirada.', 'warning')
        return redirect(url_for('login'))

    user_schema = session.get('user_schema')
    redirect_url = request.referrer or url_for('metas')
    
    if not user_schema:
        flash('Error interno.', 'danger')
        return redirect(url_for('login'))

    valor_adicionado_str = request.form.get('valor_progresso')

    if not valor_adicionado_str:
        flash('Error: Ingresa el valor a agregar al progreso de la meta.', 'danger')
        return redirect(redirect_url)

    try:
        valor_adicionado = Decimal(valor_adicionado_str.replace(',', '.'))
        if valor_adicionado <= 0:
            raise ValueError("El valor agregado a la meta debe ser positivo.")
    except (InvalidOperation, ValueError) as e:
        flash(f'Error: Valor de progreso inválido. {e}', 'danger')
        return redirect(redirect_url)

    conn = get_db_connection()
    if not conn:
        flash('Error de conexión con la base de datos.', 'danger')
        return redirect(redirect_url)

    cur = None
    try:
        cur = conn.cursor(cursor_factory=DictCursor)
        query_meta = sql.SQL("""
            SELECT id, descricao, valor_alvo, valor_atual 
            FROM {schema}.metas 
            WHERE id = %s AND status = 'ativa'
        """).format(schema=sql.Identifier(user_schema))
        cur.execute(query_meta, (meta_id,))
        meta = cur.fetchone()

        if not meta:
            flash('Meta no encontrada o no está activa.', 'warning')
            return redirect(redirect_url)

        novo_valor_atual = meta['valor_atual'] + valor_adicionado
        status_meta_final = 'ativa'
        
        if novo_valor_atual >= meta['valor_alvo']:
            novo_valor_atual = meta['valor_alvo']
            status_meta_final = 'concluida'
            flash(f'¡Felicidades! Meta "{meta["descricao"]}" completada con {format_currency_filter(meta["valor_alvo"])}!', 'success')
        else:
            flash(f'¡Progreso de {format_currency_filter(valor_adicionado)} agregado a la meta "{meta["descricao"]}"!', 'success')

        update_query = sql.SQL("""
            UPDATE {schema}.metas
            SET valor_atual = %s, status = %s, atualizado_em = NOW()
            WHERE id = %s
        """).format(schema=sql.Identifier(user_schema))
        cur.execute(update_query, (novo_valor_atual, status_meta_final, meta_id))
        conn.commit()

    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash(f'Error al agregar progreso: {e}', 'danger')
        logging.error(f"Error DB al agregar progreso meta ID {meta_id} para schema {user_schema}: {e}")
    except Exception as e:
        if conn: conn.rollback()
        flash(f'Error inesperado al agregar progreso: {e}', 'danger')
        logging.error(f"Error inesperado al agregar progreso meta ID {meta_id} para schema {user_schema}: {e}")
    finally:
        if cur: cur.close()
        if conn: conn.close()

    return redirect(redirect_url)

@app.route('/metas/<int:meta_id>/cancelar', methods=['POST'])
def cancelar_meta(meta_id):
    if 'user_assinatura_id' not in session:
        flash('Sesión expirada.', 'warning')
        return redirect(url_for('login'))

    user_schema = session.get('user_schema')
    redirect_url = url_for('metas')  # Siempre regresa a metas
    
    if not user_schema:
        flash('Error interno.', 'danger')
        session.clear()
        return redirect(url_for('login'))

    conn = get_db_connection()
    if not conn:
        flash('Error de conexión con la base de datos.', 'danger')
        return redirect(redirect_url)

    cur = None
    try:
        cur = conn.cursor(cursor_factory=DictCursor)
        
        # Buscar información de la meta antes de eliminar
        query_meta = sql.SQL("SELECT descricao, status FROM {schema}.metas WHERE id = %s").format(schema=sql.Identifier(user_schema))
        cur.execute(query_meta, (meta_id,))
        meta = cur.fetchone()
        
        if not meta:
            flash('Meta no encontrada.', 'warning')
            return redirect(redirect_url)
            
        if meta['status'] != 'ativa':
            flash('Solo las metas activas pueden ser eliminadas.', 'warning')
            return redirect(redirect_url)

        # Eliminar permanentemente la meta
        delete_query = sql.SQL("DELETE FROM {schema}.metas WHERE id = %s").format(schema=sql.Identifier(user_schema))
        cur.execute(delete_query, (meta_id,))
        conn.commit()

        if cur.rowcount > 0:
            flash(f'¡Meta "{meta["descricao"]}" eliminada con éxito!', 'success')
            logging.info(f"Meta ID {meta_id} eliminada permanentemente del schema {user_schema}")
        else:
            flash('No se pudo eliminar la meta.', 'warning')
            logging.warning(f"Meta ID {meta_id} no se pudo eliminar del schema {user_schema}")

    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash('Error en la base de datos al eliminar meta.', 'danger')
        logging.error(f"Error DB eliminar meta ID {meta_id} {user_schema}: {e}")
    except Exception as e:
        if conn: conn.rollback()
        flash('Error inesperado al eliminar meta.', 'danger')
        logging.error(f"Error inesperado eliminar meta ID {meta_id} {user_schema}: {e}", exc_info=True)
    finally:
        if cur: cur.close()
        if conn: conn.close()
        
    return redirect(redirect_url)

@app.route('/metas/<int:meta_id>/delete', methods=['POST'])
def delete_meta(meta_id):
    if 'user_assinatura_id' not in session:
        flash('Sesión expirada.', 'warning')
        return redirect(url_for('login'))

    user_schema = session.get('user_schema')
    redirect_url = url_for('metas')
    
    if not user_schema:
        flash('Error interno.', 'danger')
        session.clear()
        return redirect(url_for('login'))

    conn = get_db_connection()
    if not conn:
        flash('Error de conexión con la base de datos.', 'danger')
        return redirect(redirect_url)

    cur = None
    try:
        cur = conn.cursor(cursor_factory=DictCursor)
        
        # Buscar información de la meta antes de eliminar
        query_meta = sql.SQL("SELECT descricao FROM {schema}.metas WHERE id = %s").format(schema=sql.Identifier(user_schema))
        cur.execute(query_meta, (meta_id,))
        meta = cur.fetchone()
        
        if not meta:
            flash('Meta no encontrada.', 'warning')
            return redirect(redirect_url)

        # Eliminar permanentemente la meta
        delete_query = sql.SQL("DELETE FROM {schema}.metas WHERE id = %s").format(schema=sql.Identifier(user_schema))
        cur.execute(delete_query, (meta_id,))
        conn.commit()

        if cur.rowcount > 0:
            flash(f'¡Meta "{meta["descricao"]}" eliminada permanentemente!', 'success')
            logging.info(f"Meta ID {meta_id} eliminada permanentemente del schema {user_schema}")
        else:
            flash('No se pudo eliminar la meta.', 'warning')
            logging.warning(f"Meta ID {meta_id} no se pudo eliminar del schema {user_schema}")

    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash('Error en la base de datos al eliminar meta.', 'danger')
        logging.error(f"Error DB eliminar meta ID {meta_id} {user_schema}: {e}")
    except Exception as e:
        if conn: conn.rollback()
        flash('Error inesperado al eliminar meta.', 'danger')
        logging.error(f"Error inesperado eliminar meta ID {meta_id} {user_schema}: {e}", exc_info=True)
    finally:
        if cur: cur.close()
        if conn: conn.close()
        
    return redirect(redirect_url)


# --- Rota Relatórios (Com Correção de Sintaxe e Logs) ---
@app.route('/metodos-pagamento')
def metodos_pagamento():
    """Exibe a página de gerenciamento de métodos de pagamento."""
    if 'user_assinatura_id' not in session:
        flash('Você precisa fazer login para acessar esta página.', 'warning')
        return redirect(url_for('login'))

    user_schema = session.get('user_schema')
    user_nome = session.get('user_nome', session.get('user_email'))
    if not user_schema:
        flash('Erro interno: Informações do usuário incompletas.', 'danger')
        session.clear(); return redirect(url_for('login'))

    lista_metodos = []
    conn = get_db_connection()
    if not conn:
        flash('Erro de conexão com o banco de dados.', 'danger')
    else:
        cur = None
        try:
            cur = conn.cursor(cursor_factory=DictCursor)
            query = sql.SQL("""
                SELECT id, nome, tipo, modalidad, ativo, criado_em
                FROM {schema}.metodos_pagamento
                ORDER BY ativo DESC, nome ASC
            """).format(schema=sql.Identifier(user_schema))
            cur.execute(query)
            lista_metodos = cur.fetchall()
            logging.info(f"Métodos de pagamento buscados para schema {user_schema}: {len(lista_metodos)} encontrados.")
        except psycopg2.Error as e:
            flash('Erro ao buscar métodos de pagamento no banco de dados.', 'danger')
            logging.error(f"Erro DB ao buscar métodos de pagamento (schema {user_schema}): {e}")
        except Exception as e:
            flash('Ocorreu um erro inesperado ao buscar métodos de pagamento.', 'danger')
            logging.error(f"Erro inesperado ao buscar métodos de pagamento (schema {user_schema}): {e}", exc_info=True)
        finally:
            if cur: cur.close()
            if conn: conn.close()

    return render_template('metodos_pagamento.html',
                           user_nome=user_nome,
                           metodos=lista_metodos)


@app.route('/metodos-pagamento/add', methods=['POST'])
def add_metodo_pagamento():
    """Processa o formulário de adição de novo método de pagamento."""
    if 'user_assinatura_id' not in session:
        flash('Sessão expirada.', 'warning'); return redirect(url_for('login'))

    user_schema = session.get('user_schema')
    redirect_url = url_for('metodos_pagamento')
    if not user_schema:
        flash('Erro interno.', 'danger'); session.clear(); return redirect(url_for('login'))

    nome_metodo = request.form.get('metodo_nome')
    tipo_metodo = request.form.get('metodo_tipo')
    modalidad_metodo = request.form.get('metodo_modalidad')

    if not nome_metodo or not tipo_metodo:
        flash('Nome e Tipo do método de pagamento são obrigatórios.', 'danger')
        return redirect(redirect_url)

    valid_tipos = ['efectivo', 'tarjeta', 'digital', 'voucher', 'transferencia']
    if tipo_metodo not in valid_tipos:
        flash('Tipo de método de pagamento inválido.', 'danger')
        return redirect(redirect_url)

    # Modalidad é opcional, mas se informada deve ser válida
    if modalidad_metodo and modalidad_metodo not in ['debito', 'credito', 'na']:
        flash('Modalidade inválida.', 'danger')
        return redirect(redirect_url)

    # Se modalidad está vazia, define como 'na' (padrão do banco)
    if not modalidad_metodo:
        modalidad_metodo = 'na'

    conn = get_db_connection()
    if not conn:
        flash('Erro de conexão com o banco.', 'danger'); return redirect(redirect_url)

    cur = None
    try:
        cur = conn.cursor()
        insert_query = sql.SQL("""
            INSERT INTO {schema}.metodos_pagamento (nome, tipo, modalidad)
            VALUES (%s, %s, %s)
        """).format(schema=sql.Identifier(user_schema))
        cur.execute(insert_query, (nome_metodo.strip(), tipo_metodo, modalidad_metodo))
        conn.commit()
        flash('¡Método de pago agregado con éxito!', 'success')
        logging.info(f"Método de pagamento '{nome_metodo}' ({tipo_metodo}) adicionado para schema {user_schema}")

    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        flash(f'Error: Ya existe un método de pago con el nombre "{nome_metodo}" y tipo "{tipo_metodo}".', 'danger')
        logging.warning(f"Tentativa de adicionar método de pagamento duplicado: '{nome_metodo}' ({tipo_metodo}) para schema {user_schema}")
    except psycopg2.Error as e:
        conn.rollback()
        flash('Error en la base de datos.', 'danger')
        logging.error(f"Erro DB ao adicionar método de pagamento (schema {user_schema}): {e}")
    except Exception as e:
        conn.rollback()
        flash('Error inesperado.', 'danger')
        logging.error(f"Erro inesperado ao adicionar método de pagamento (schema {user_schema}): {e}", exc_info=True)
    finally:
        if cur: cur.close()
        if conn: conn.close()

    return redirect(redirect_url)


@app.route('/metodos-pagamento/<int:metodo_id>/edit', methods=['POST'])
def edit_metodo_pagamento(metodo_id):
    """Processa o formulário de edição de método de pagamento."""
    if 'user_assinatura_id' not in session:
        flash('Sessão expirada.', 'warning'); return redirect(url_for('login'))

    user_schema = session.get('user_schema')
    redirect_url = url_for('metodos_pagamento')
    if not user_schema:
        flash('Erro interno.', 'danger'); session.clear(); return redirect(url_for('login'))

    nome_metodo = request.form.get('edit_metodo_nome')
    tipo_metodo = request.form.get('edit_metodo_tipo')
    modalidad_metodo = request.form.get('edit_metodo_modalidad')
    ativo = request.form.get('edit_metodo_ativo') == 'on'

    if not nome_metodo or not tipo_metodo:
        flash('Nome e Tipo são obrigatórios para editar.', 'danger')
        return redirect(redirect_url)

    valid_tipos = ['efectivo', 'tarjeta', 'digital', 'voucher', 'transferencia']
    if tipo_metodo not in valid_tipos:
        flash('Tipo inválido.', 'danger')
        return redirect(redirect_url)

    if modalidad_metodo and modalidad_metodo not in ['debito', 'credito', 'na']:
        flash('Modalidade inválida.', 'danger')
        return redirect(redirect_url)

    if not modalidad_metodo:
        modalidad_metodo = 'na'

    conn = get_db_connection()
    if not conn:
        flash('Erro de conexão.', 'danger'); return redirect(redirect_url)

    cur = None
    try:
        cur = conn.cursor()
        update_query = sql.SQL("""
            UPDATE {schema}.metodos_pagamento
            SET nome = %s, tipo = %s, modalidad = %s, ativo = %s, atualizado_em = NOW()
            WHERE id = %s
        """).format(schema=sql.Identifier(user_schema))
        cur.execute(update_query, (nome_metodo.strip(), tipo_metodo, modalidad_metodo, ativo, metodo_id))
        conn.commit()

        if cur.rowcount > 0:
            flash('¡Método de pago actualizado con éxito!', 'success')
            logging.info(f"Método de pagamento ID {metodo_id} atualizado para '{nome_metodo}' no schema {user_schema}")
        else:
            flash('Método de pago no encontrado.', 'warning')

    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        flash(f'Error: Ya existe otro método de pago con el nombre "{nome_metodo}" y tipo "{tipo_metodo}".', 'danger')
    except psycopg2.Error as e:
        conn.rollback()
        flash('Error en la base de datos.', 'danger')
        logging.error(f"Erro DB ao editar método de pagamento ID {metodo_id} (schema {user_schema}): {e}")
    except Exception as e:
        conn.rollback()
        flash('Error inesperado.', 'danger')
        logging.error(f"Erro inesperado ao editar método de pagamento ID {metodo_id} (schema {user_schema}): {e}", exc_info=True)
    finally:
        if cur: cur.close()
        if conn: conn.close()

    return redirect(redirect_url)


@app.route('/metodos-pagamento/<int:metodo_id>/delete', methods=['POST'])
def delete_metodo_pagamento(metodo_id):
    """Processa a exclusão de um método de pagamento."""
    if 'user_assinatura_id' not in session:
        flash('Sessão expirada.', 'warning'); return redirect(url_for('login'))

    user_schema = session.get('user_schema')
    redirect_url = url_for('metodos_pagamento')
    if not user_schema:
        flash('Erro interno.', 'danger'); return redirect(url_for('login'))

    conn = get_db_connection()
    if not conn:
        flash('Erro de conexão.', 'danger'); return redirect(redirect_url)

    cur = None
    try:
        cur = conn.cursor(cursor_factory=DictCursor)

        # Verifica se o método existe
        check_query = sql.SQL("SELECT nome FROM {schema}.metodos_pagamento WHERE id = %s").format(schema=sql.Identifier(user_schema))
        cur.execute(check_query, (metodo_id,))
        metodo_info = cur.fetchone()

        if not metodo_info:
            flash('Método de pago no encontrado.', 'warning')
            return redirect(redirect_url)

        metodo_nome = metodo_info['nome']

        # Verifica uso em transações (quando implementar o campo metodo_pagamento nas outras tabelas)
        # Por enquanto, permite a exclusão sempre

        delete_query = sql.SQL("DELETE FROM {schema}.metodos_pagamento WHERE id = %s").format(schema=sql.Identifier(user_schema))
        cur.execute(delete_query, (metodo_id,))
        conn.commit()

        if cur.rowcount > 0:
            flash('¡Método de pago eliminado con éxito!', 'success')
            logging.info(f"Método de pagamento ID {metodo_id} ('{metodo_nome}') excluído do schema {user_schema}")
        else:
            flash('Método de pago no encontrado.', 'warning')

    except psycopg2.Error as e:
        conn.rollback()
        flash('Error en la base de datos.', 'danger')
        logging.error(f"Erro DB ao excluir método de pagamento ID {metodo_id} (schema {user_schema}): {e}")
    except Exception as e:
        conn.rollback()
        flash('Error inesperado.', 'danger')
        logging.error(f"Erro inesperado ao excluir método de pagamento ID {metodo_id} (schema {user_schema}): {e}", exc_info=True)
    finally:
        if cur: cur.close()
        if conn: conn.close()

    return redirect(redirect_url)


# --- ROTAS PARA NÚMEROS COMPARTILHADOS ---

@app.route('/numeros-compartilhados')
def numeros_compartilhados():
    """Exibe a página de gerenciamento de números compartilhados."""
    if 'user_assinatura_id' not in session:
        # MENSAGEM ATUALIZADA
        flash('Necesita iniciar sesión para acceder a esta página.', 'warning')
        return redirect(url_for('login'))

    user_schema = session.get('user_schema')
    user_nome = session.get('user_nome', session.get('user_email'))
    if not user_schema:
        # MENSAGEM ATUALIZADA
        flash('Error interno: Información del usuario incompleta.', 'danger')
        session.clear()
        return redirect(url_for('login'))

    lista_numeros = []
    conn = get_db_connection()
    if not conn:
        # MENSAGEM ATUALIZADA
        flash('Error de conexión con la base de datos.', 'danger')
    else:
        cur = None
        try:
            cur = conn.cursor(cursor_factory=DictCursor)
            # Garante que a tabela exista antes de consultar
            create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {schema}.numero_compartilhado (
                id SERIAL PRIMARY KEY,
                numero_whatsapp VARCHAR(20) NOT NULL,
                nome VARCHAR(100) NOT NULL,
                criado_em TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                ativo BOOLEAN DEFAULT TRUE
            )
            """).format(schema=sql.Identifier(user_schema))
            cur.execute(create_table_query)
            # Garante que o índice exista
            create_index_query = sql.SQL("""
            CREATE INDEX IF NOT EXISTS idx_numero_compartilhado_numero 
            ON {schema}.numero_compartilhado(numero_whatsapp)
            """).format(schema=sql.Identifier(user_schema))
            cur.execute(create_index_query)
            conn.commit()

            # Busca os dados
            query = sql.SQL("""
                SELECT id, numero_whatsapp, nome, criado_em, ativo
                FROM {schema}.numero_compartilhado
                ORDER BY nome ASC
            """).format(schema=sql.Identifier(user_schema))
            cur.execute(query)
            lista_numeros = cur.fetchall()
            logging.info(f"Números compartilhados buscados para schema {user_schema}: {len(lista_numeros)} encontrados.")
        except psycopg2.Error as e:
            conn.rollback()
            # MENSAGEM ATUALIZADA
            flash('Error al buscar números compartidos en la base de datos.', 'danger')
            logging.error(f"Erro DB ao buscar números compartilhados (schema {user_schema}): {e}")
        except Exception as e:
            conn.rollback()
            # MENSAGEM ATUALIZADA
            flash('Ocurrió un error inesperado al buscar números compartidos.', 'danger')
            logging.error(f"Erro inesperado ao buscar números compartilhados (schema {user_schema}): {e}", exc_info=True)
        finally:
            if cur: cur.close()
            if conn: conn.close()

    return render_template('numeros_compartilhados.html',
                           user_nome=user_nome,
                           numeros=lista_numeros)

@app.route('/numeros-compartilhados/add', methods=['POST'])
def add_numero_compartilhado():
    """Processa o formulário de adição de novo número compartilhado."""
    if 'user_assinatura_id' not in session:
        # MENSAGEM ATUALIZADA
        flash('Sesión expirada.', 'warning')
        return redirect(url_for('login'))

    user_schema = session.get('user_schema')
    redirect_url = url_for('numeros_compartilhados')
    if not user_schema:
        # MENSAGEM ATUALIZADA
        flash('Error interno.', 'danger')
        session.clear()
        return redirect(url_for('login'))

    numero_whatsapp = request.form.get('numero_whatsapp')
    nome = request.form.get('nome')

    if not numero_whatsapp or not nome:
        # MENSAGEM ATUALIZADA
        flash('El número de WhatsApp y el Nombre son obligatorios.', 'danger')
        return redirect(redirect_url)

    conn = get_db_connection()
    if not conn:
        # MENSAGEM ATUALIZADA
        flash('Error de conexión con la base de datos.', 'danger')
        return redirect(redirect_url)

    cur = None
    try:
        cur = conn.cursor()
        insert_query = sql.SQL("""
            INSERT INTO {schema}.numero_compartilhado (numero_whatsapp, nome)
            VALUES (%s, %s)
        """).format(schema=sql.Identifier(user_schema))
        cur.execute(insert_query, (numero_whatsapp.strip(), nome.strip()))
        conn.commit()
        # MENSAGEM ATUALIZADA
        flash('¡Número compartido agregado con éxito!', 'success')
        logging.info(f"Número compartilhado '{nome}' ({numero_whatsapp}) adicionado para schema {user_schema}")

    except psycopg2.Error as e:
        conn.rollback()
        # MENSAGEM ATUALIZADA
        flash('Error en la base de datos al agregar el número.', 'danger')
        logging.error(f"Erro DB ao adicionar número compartilhado (schema {user_schema}): {e}")
    except Exception as e:
        conn.rollback()
        # MENSAGEM ATUALIZADA
        flash('Ocurrió un error inesperado.', 'danger')
        logging.error(f"Erro inesperado ao adicionar número compartilhado (schema {user_schema}): {e}", exc_info=True)
    finally:
        if cur: cur.close()
        if conn: conn.close()

    return redirect(redirect_url)


@app.route('/numeros-compartilhados/<int:numero_id>/edit', methods=['POST'])
def edit_numero_compartilhado(numero_id):
    """Processa o formulário de edição de número compartilhado."""
    if 'user_assinatura_id' not in session:
        # MENSAGEM ATUALIZADA
        flash('Sesión expirada.', 'warning')
        return redirect(url_for('login'))

    user_schema = session.get('user_schema')
    redirect_url = url_for('numeros_compartilhados')
    if not user_schema:
        # MENSAGEM ATUALIZADA
        flash('Error interno.', 'danger')
        session.clear()
        return redirect(url_for('login'))

    numero_whatsapp = request.form.get('numero_whatsapp')
    nome = request.form.get('nome')
    ativo = request.form.get('ativo') == 'on'

    if not numero_whatsapp or not nome:
        # MENSAGEM ATUALIZADA
        flash('El número de WhatsApp y el Nombre son obligatorios para editar.', 'danger')
        return redirect(redirect_url)

    conn = get_db_connection()
    if not conn:
        # MENSAGEM ATUALIZADA
        flash('Error de conexión.', 'danger')
        return redirect(redirect_url)

    cur = None
    try:
        cur = conn.cursor()
        update_query = sql.SQL("""
            UPDATE {schema}.numero_compartilhado
            SET numero_whatsapp = %s, nome = %s, ativo = %s
            WHERE id = %s
        """).format(schema=sql.Identifier(user_schema))
        cur.execute(update_query, (numero_whatsapp.strip(), nome.strip(), ativo, numero_id))
        conn.commit()

        if cur.rowcount > 0:
            # MENSAGEM ATUALIZADA
            flash('¡Número compartido actualizado con éxito!', 'success')
            logging.info(f"Número compartilhado ID {numero_id} atualizado para '{nome}' no schema {user_schema}")
        else:
            # MENSAGEM ATUALIZADA
            flash('Número compartido no encontrado.', 'warning')

    except psycopg2.Error as e:
        conn.rollback()
        # MENSAGEM ATUALIZADA
        flash('Error en la base de datos al editar el número.', 'danger')
        logging.error(f"Erro DB ao editar número compartilhado ID {numero_id} (schema {user_schema}): {e}")
    except Exception as e:
        conn.rollback()
        # MENSAGEM ATUALIZADA
        flash('Ocurrió un error inesperado al editar el número.', 'danger')
        logging.error(f"Erro inesperado ao editar número ID {numero_id} (schema {user_schema}): {e}", exc_info=True)
    finally:
        if cur: cur.close()
        if conn: conn.close()

    return redirect(redirect_url)

@app.route('/numeros-compartilhados/<int:numero_id>/delete', methods=['POST'])
def delete_numero_compartilhado(numero_id):
    """Processa a exclusão de um número compartilhado."""
    if 'user_assinatura_id' not in session:
        # MENSAGEM ATUALIZADA
        flash('Sesión expirada.', 'warning')
        return redirect(url_for('login'))

    user_schema = session.get('user_schema')
    redirect_url = url_for('numeros_compartilhados')
    if not user_schema:
        # MENSAGEM ATUALIZADA
        flash('Error interno.', 'danger')
        return redirect(url_for('login'))

    conn = get_db_connection()
    if not conn:
        # MENSAGEM ATUALIZADA
        flash('Error de conexión.', 'danger')
        return redirect(redirect_url)

    cur = None
    try:
        cur = conn.cursor()
        delete_query = sql.SQL("DELETE FROM {schema}.numero_compartilhado WHERE id = %s").format(schema=sql.Identifier(user_schema))
        cur.execute(delete_query, (numero_id,))
        conn.commit()

        if cur.rowcount > 0:
            # MENSAGEM ATUALIZADA (Esta é a da imagem!)
            flash('¡Número compartido eliminado con éxito!', 'success')
            logging.info(f"Número compartilhado ID {numero_id} excluído do schema {user_schema}")
        else:
            # MENSAGEM ATUALIZADA
            flash('Número compartido no encontrado.', 'warning')

    except psycopg2.Error as e:
        conn.rollback()
        # MENSAGEM ATUALIZADA
        flash('Error en la base de datos al eliminar el número. Verifique que no esté en uso.', 'danger')
        logging.error(f"Erro DB ao excluir número compartilhado ID {numero_id} (schema {user_schema}): {e}")
    except Exception as e:
        conn.rollback()
        # MENSAGEM ATUALIZADA
        flash('Ocurrió un error inesperado al eliminar el número.', 'danger')
        logging.error(f"Erro inesperado ao excluir número ID {numero_id} (schema {user_schema}): {e}", exc_info=True)
    finally:
        if cur: cur.close()
        if conn: conn.close()

    return redirect(redirect_url)


@app.route('/relatorios')
def relatorios():
    if 'user_assinatura_id' not in session:
        flash('Você precisa fazer login para acessar esta página.', 'warning')
        return redirect(url_for('login'))

    user_schema = session.get('user_schema')
    user_nome = session.get('user_nome', session.get('user_email'))
    if not user_schema:
        flash('Erro interno: Informações do usuário incompletas.', 'danger')
        session.clear()
        return redirect(url_for('login'))

    # --- 1. Processamento de Filtros com Múltiplos Tipos ---
    today = date.today()
    default_start_date = today.replace(day=1)
    default_end_date = today
    
    data_inicio_str = request.args.get('data_inicio', default_start_date.strftime('%Y-%m-%d'))
    data_fim_str = request.args.get('data_fim', default_end_date.strftime('%Y-%m-%d'))
    
    # NOVO: Recebe uma LISTA de tipos dos checkboxes. Se nada for enviado, usa todos.
    tipos_transacao_selecionados = request.args.getlist('tipo_transacao')
    if not tipos_transacao_selecionados:
        tipos_transacao_selecionados = ['receitas', 'gastos_variaveis', 'gastos_fixos']

    categoria_filtro = request.args.get('categoria_filtro', 'todas')
    
    try: data_inicio = datetime.strptime(data_inicio_str, '%Y-%m-%d').date()
    except (ValueError, TypeError): data_inicio = default_start_date
    try: data_fim = datetime.strptime(data_fim_str, '%Y-%m-%d').date()
    except (ValueError, TypeError): data_fim = default_end_date

    filtros_aplicados = {
        'data_inicio_raw': data_inicio_str, 'data_fim_raw': data_fim_str,
        'tipos_transacao': tipos_transacao_selecionados, # <-- NOVO: Agora é uma lista
        'categoria_filtro': categoria_filtro
    }

    # --- 2. Inicialização dos Dados ---
    dados_relatorio = { "total_receitas": Decimal('0.00'), "total_despesas": Decimal('0.00') }
    dados_grafico = { "labels": [], "datasets": { "receitas": [], "despesas": [] } }
    categorias_disponiveis = {'receitas': [], 'variaveis': [], 'fixas': []}
    transacoes_raw = []
    
    conn = get_db_connection()
    if not conn:
        flash('Erro de conexão com o banco.', 'danger')
        return render_template('relatorios.html', user_nome=user_nome, filtros_aplicados=filtros_aplicados, transacoes_agrupadas={}, dados_relatorio=dados_relatorio, dados_grafico=dados_grafico, categorias_disponiveis=categorias_disponiveis)

    cur = None
    try:
        cur = conn.cursor(cursor_factory=DictCursor)
        
        # --- 3. Buscar Dados com Base nos Filtros (Lógica de Múltipla Seleção) ---
        # Popula as categorias para o modal de filtro
        categorias_disponiveis['receitas'] = buscar_categorias_por_tipo(conn, user_schema, 'receita')
        categorias_disponiveis['variaveis'] = buscar_categorias_por_tipo(conn, user_schema, 'gasto_variavel')
        categorias_disponiveis['fixas'] = buscar_categorias_por_tipo(conn, user_schema, 'gasto_fixo')

        # Constrói a lista de transações baseada nos checkboxes selecionados
        if 'receitas' in tipos_transacao_selecionados:
            where = [sql.SQL("fecha BETWEEN %s AND %s")]; params = [data_inicio, data_fim]
            if categoria_filtro != 'todas' and len(tipos_transacao_selecionados) == 1:
                where.append(sql.SQL("categoria = %s")); params.append(categoria_filtro)
            query = sql.SQL("SELECT id, fecha as data, descripcion, categoria, valor, 'receita' as tipo FROM {schema}.outras_receitas WHERE {where}").format(schema=sql.Identifier(user_schema), where=sql.SQL(' AND ').join(where))
            cur.execute(query, params)
            transacoes_raw.extend([dict(r) for r in cur.fetchall()])

        if 'gastos_variaveis' in tipos_transacao_selecionados:
            where = [sql.SQL("data BETWEEN %s AND %s")]; params = [data_inicio, data_fim]
            if categoria_filtro != 'todas' and len(tipos_transacao_selecionados) == 1:
                where.append(sql.SQL("categoria = %s")); params.append(categoria_filtro)
            query = sql.SQL("SELECT id, data, descripcion, categoria, valor, 'gasto_variavel' as tipo FROM {schema}.gastos WHERE {where}").format(schema=sql.Identifier(user_schema), where=sql.SQL(' AND ').join(where))
            cur.execute(query, params)
            transacoes_raw.extend([dict(r) for r in cur.fetchall()])

        if 'gastos_fixos' in tipos_transacao_selecionados:
            query = sql.SQL("SELECT id, fecha_inicio, descripcion, categoria, valor, recurrencia FROM {schema}.gastos_fixos WHERE activo = TRUE AND fecha_inicio <= %s").format(schema=sql.Identifier(user_schema))
            cur.execute(query, (data_fim,))
            for gf in cur.fetchall():
                if categoria_filtro != 'todas' and len(tipos_transacao_selecionados) == 1 and gf['categoria'] != categoria_filtro: continue
                rrule_params = get_rrule_params(gf['recurrencia'])
                if rrule_params:
                    for occ in rrule(dtstart=gf['fecha_inicio'], until=data_fim, **rrule_params):
                        if occ.date() >= data_inicio:
                            transacoes_raw.append({'id': gf['id'], 'data': occ.date(), 'descripcion': gf['descripcion'], 'categoria': gf['categoria'], 'valor': gf['valor'], 'tipo': 'gasto_fixo'})
        
        # --- 4. Calcular Totais para Stat Cards e Gráfico (lógica inalterada, já busca tudo) ---
        # (O código para calcular totais e dados do gráfico permanece o mesmo da sua versão original)
        dias_no_periodo = [data_inicio + timedelta(days=i) for i in range((data_fim - data_inicio).days + 1)]
        receitas_diarias = {d: Decimal(0) for d in dias_no_periodo}
        despesas_diarias = {d: Decimal(0) for d in dias_no_periodo}
        cur.execute(sql.SQL("SELECT fecha, valor FROM {schema}.outras_receitas WHERE fecha BETWEEN %s AND %s").format(schema=sql.Identifier(user_schema)), (data_inicio, data_fim))
        for r in cur.fetchall(): dados_relatorio['total_receitas'] += r['valor']; receitas_diarias[r['fecha']] += r['valor']
        cur.execute(sql.SQL("SELECT data, valor FROM {schema}.gastos WHERE data BETWEEN %s AND %s").format(schema=sql.Identifier(user_schema)), (data_inicio, data_fim))
        for gv in cur.fetchall(): dados_relatorio['total_despesas'] += gv['valor']; despesas_diarias[gv['data']] += gv['valor']
        cur.execute(sql.SQL("SELECT valor, fecha_inicio, recurrencia FROM {schema}.gastos_fixos WHERE activo = TRUE AND fecha_inicio <= %s").format(schema=sql.Identifier(user_schema)), (data_fim,))
        for gf in cur.fetchall():
            rrule_params = get_rrule_params(gf['recurrencia'])
            if rrule_params:
                for occ in rrule(dtstart=gf['fecha_inicio'], until=data_fim, **rrule_params):
                    if occ.date() >= data_inicio: dados_relatorio['total_despesas'] += gf['valor']; despesas_diarias[occ.date()] += gf['valor']
        dados_grafico['labels'] = [d.strftime('%d/%m') for d in dias_no_periodo]
        dados_grafico['datasets']['receitas'] = [float(v) for v in receitas_diarias.values()]
        dados_grafico['datasets']['despesas'] = [float(v) for v in despesas_diarias.values()]
        
        # --- 5. Agrupar Resultados para a Lista ---
        transacoes_raw.sort(key=itemgetter('data'), reverse=True)
        transacoes_agrupadas = {data: list(grupo) for data, grupo in groupby(transacoes_raw, key=itemgetter('data'))}

    except Exception as e:
        flash('Ocorreu um erro ao gerar o relatório.', 'danger')
        logging.error(f"Erro ao gerar relatório para {user_schema}: {e}", exc_info=True)
        transacoes_agrupadas = {}
    finally:
        if cur: cur.close()
        if conn: conn.close()

    return render_template('relatorios.html',
                           user_nome=user_nome,
                           dados_relatorio=dados_relatorio,
                           dados_grafico=dados_grafico,
                           categorias_disponiveis=categorias_disponiveis,
                           filtros_aplicados=filtros_aplicados,
                           transacoes_agrupadas=transacoes_agrupadas,
                           hoje=date.today(),
                           ontem=date.today() - timedelta(days=1))




# ... (resto do app.py, incluindo if __name__ == '__main__':) ...
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 3333))
    # Certifique-se de que debug=False em produção
    app.run(host='0.0.0.0', port=port, debug=True)
