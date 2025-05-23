# -*- coding: utf-8 -*-
import os
import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
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
            user=os.environ.get('DB_USER'),
            password=os.environ.get('DB_PASSWORD')
        )
        return conn
    except psycopg2.Error as e:
        logging.error(f"Erro ao conectar ao PostgreSQL: {e}")
        if conn: conn.close()
        return None

# --- Funções Auxiliares ---
# As funções gerar_hash_senha, verificar_senha, e gerar_nome_schema foram movidas para auth.py

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



# -*- coding: utf-8 -*-
# ... (outras importações no topo do seu app.py)
# Certifique-se de que estas importações de dateutil estão presentes:
from dateutil.rrule import rrule, MONTHLY, YEARLY, DAILY # Você já deve ter WEEKLY se descomentar o código
# from dateutil.rrule import WEEKLY # Descomente se for usar 'semanal'

# ... (resto do seu código Flask: app = Flask(...), get_db_connection(), etc.)

# Função auxiliar para mapear recorrência (VERSÃO ATUALIZADA) - A DEFINIÇÃO DUPLICADA FOI REMOVIDA DE CIMA
# A definição restante está mais abaixo no código e será mantida.


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
    if value is None or not isinstance(value, (date, datetime)): return ""
    try: return value.strftime(format_str)
    except ValueError: return str(value)
app.jinja_env.filters['date'] = format_date_filter

# --- Função para converter tipos não serializáveis em JSON ---
def json_converter(obj):
    if isinstance(obj, Decimal): return float(obj)
    if isinstance(obj, (date, datetime)): return obj.isoformat()
    if isinstance(obj, bool): return obj
    try: return str(obj)
    except Exception: return None





# --- Rotas ---
# --- Rotas ---
# As rotas /login, /criar-conta, /logout, /esqueci-senha foram movidas para auth.py
@app.route('/')
def index():
    if 'user_assinatura_id' in session: 
        return redirect(url_for('dashboard')) # 'dashboard' permanece em app.py
    return redirect(url_for('auth.login')) # Atualizado para o blueprint


@app.route('/receitas/<int:item_id>/edit', methods=['POST'])
def edit_outra_receita(item_id):
    """
    Processa a submissão do formulário de edição de uma 'Outra Receita'.
    """
    if 'user_assinatura_id' not in session:
        flash('Sessão expirada. Por favor, faça login novamente.', 'warning')
        return redirect(url_for('auth.login')) # Atualizado

    user_schema = session.get('user_schema')
    redirect_url = url_for('receitas') # Redireciona de volta para a lista de receitas

    if not user_schema:
        flash('Erro interno: Schema do usuário não encontrado.', 'danger')
        session.clear()
        return redirect(url_for('auth.login')) # Atualizado

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
    """Exibe a página de gerenciamento de categorias."""
    if 'user_assinatura_id' not in session:
        flash('Você precisa fazer login para acessar esta página.', 'warning')
        return redirect(url_for('auth.login')) # Atualizado

    user_schema = session.get('user_schema')
    user_nome = session.get('user_nome', session.get('user_email'))
    if not user_schema:
        flash('Erro interno: Informações do usuário incompletas.', 'danger')
        session.clear(); return redirect(url_for('auth.login')) # Atualizado

    lista_categorias = []
    conn = get_db_connection()
    if not conn:
        flash('Erro de conexão com o banco de dados.', 'danger')
    else:
        cur = None
        try:
            cur = conn.cursor(cursor_factory=DictCursor)
            # Busca SEM a coluna 'cor', mas incluindo 'is_fixa'
            query = sql.SQL("""
                SELECT id, nome, tipo, is_fixa
                FROM {schema}.categorias
                ORDER BY tipo, is_fixa DESC, nome ASC
            """).format(schema=sql.Identifier(user_schema))
            cur.execute(query)
            lista_categorias = cur.fetchall()
            logging.info(f"Categorias buscadas para schema {user_schema}: {len(lista_categorias)} encontradas.")
        except psycopg2.Error as e:
            flash('Erro ao buscar categorias no banco de dados.', 'danger')
            logging.error(f"Erro DB ao buscar categorias (schema {user_schema}): {e}")
        except Exception as e:
            flash('Ocorreu um erro inesperado ao buscar categorias.', 'danger')
            logging.error(f"Erro inesperado ao buscar categorias (schema {user_schema}): {e}", exc_info=True)
        finally:
            if cur: cur.close()
            if conn: conn.close()

    categorias_por_tipo = {
        'receita': [cat for cat in lista_categorias if cat['tipo'] == 'receita'],
        'gasto_variavel': [cat for cat in lista_categorias if cat['tipo'] == 'gasto_variavel'],
        'gasto_fixo': [cat for cat in lista_categorias if cat['tipo'] == 'gasto_fixo'],
    }

    return render_template('categorias.html',
                           user_nome=user_nome,
                           categorias=lista_categorias,
                           categorias_por_tipo=categorias_por_tipo)


# --- Rota para ADICIONAR Categoria (Atualizada SEM COR e com Limite) ---
@app.route('/categorias/add', methods=['POST'])
def add_categoria():
    """Processa o formulário de adição de nova categoria, com limite para gastos variáveis."""
    if 'user_assinatura_id' not in session:
        flash('Sessão expirada.', 'warning'); return redirect(url_for('auth.login')) # Atualizado

    user_schema = session.get('user_schema')
    redirect_url = url_for('categorias')
    if not user_schema:
        flash('Erro interno.', 'danger'); session.clear(); return redirect(url_for('auth.login')) # Atualizado

    nome_categoria = request.form.get('categoria_nome')
    tipo_categoria = request.form.get('categoria_tipo')
    # Cor removida

    if not nome_categoria or not tipo_categoria:
        flash('Nome e Tipo da categoria são obrigatórios.', 'danger'); return redirect(redirect_url)

    valid_tipos = ['receita', 'gasto_variavel', 'gasto_fixo']
    if tipo_categoria not in valid_tipos:
        flash('Tipo de categoria inválido.', 'danger'); return redirect(redirect_url)

    conn = get_db_connection()
    if not conn:
        flash('Erro de conexão com o banco.', 'danger'); return redirect(redirect_url)

    cur = None
    try:
        cur = conn.cursor(cursor_factory=DictCursor)

        # Lógica de limite para Gasto Variável
        if tipo_categoria == 'gasto_variavel':
            count_query = sql.SQL("""
                SELECT COUNT(*) as total
                FROM {schema}.categorias
                WHERE tipo = 'gasto_variavel' AND is_fixa = FALSE
            """).format(schema=sql.Identifier(user_schema))
            cur.execute(count_query)
            result = cur.fetchone()
            count_personalizadas_variaveis = result['total'] if result else 0

            if count_personalizadas_variaveis >= 10:
                flash('Limite atingido! Você só pode criar até 2 categorias personalizadas de Gasto Variável.', 'warning')
                if cur: cur.close()
                if conn: conn.close()
                return redirect(redirect_url)

        # Insere SEM a cor
        insert_query = sql.SQL("""
            INSERT INTO {schema}.categorias (nome, tipo)
            VALUES (%s, %s)
        """).format(schema=sql.Identifier(user_schema))
        cur.execute(insert_query, (nome_categoria.strip(), tipo_categoria))
        conn.commit()
        flash('¡Categoría agregada con éxito!', 'success')
        logging.info(f"Categoria '{nome_categoria}' ({tipo_categoria}) adicionada para schema {user_schema}")

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
        flash('Sessão expirada.', 'warning'); return redirect(url_for('auth.login')) # Atualizado

    user_schema = session.get('user_schema')
    redirect_url = url_for('categorias')
    if not user_schema:
        flash('Erro interno.', 'danger'); session.clear(); return redirect(url_for('auth.login')) # Atualizado

    nome_categoria = request.form.get('edit_categoria_nome')
    tipo_categoria = request.form.get('edit_categoria_tipo')
    # Cor removida

    if not nome_categoria or not tipo_categoria:
        flash('Nome e Tipo são obrigatórios para editar.', 'danger'); return redirect(redirect_url)

    valid_tipos = ['receita', 'gasto_variavel', 'gasto_fixo']
    if tipo_categoria not in valid_tipos:
        flash('Tipo inválido.', 'danger'); return redirect(redirect_url)

    conn = get_db_connection()
    if not conn:
        flash('Erro de conexão.', 'danger'); return redirect(redirect_url)

    cur = None
    try:
        cur = conn.cursor(cursor_factory=DictCursor)

        # Verifica se a categoria é fixa
        check_fixa_query = sql.SQL("SELECT is_fixa FROM {schema}.categorias WHERE id = %s").format(schema=sql.Identifier(user_schema))
        cur.execute(check_fixa_query, (categoria_id,))
        categoria_info = cur.fetchone()

        if not categoria_info:
             flash('Categoria não encontrada.', 'warning')
             return redirect(redirect_url)

        if categoria_info['is_fixa']:
            flash('Erro: Categorias pré-definidas não podem ser editadas.', 'danger')
            return redirect(redirect_url)

        # Atualiza SEM a cor
        update_query = sql.SQL("""
            UPDATE {schema}.categorias
            SET nome = %s, tipo = %s, atualizado_em = NOW()
            WHERE id = %s AND is_fixa = FALSE
        """).format(schema=sql.Identifier(user_schema))
        cur.execute(update_query, (nome_categoria.strip(), tipo_categoria, categoria_id))
        conn.commit()

        if cur.rowcount > 0:
            flash('¡Categoría actualizada con éxito!', 'success')
            logging.info(f"Categoria ID {categoria_id} atualizada para '{nome_categoria}' ({tipo_categoria}) no schema {user_schema}")
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


# --- Rota para EXCLUIR Categoria (sem alterações, já não usava cor) ---
@app.route('/categorias/<int:categoria_id>/delete', methods=['POST'])
def delete_categoria(categoria_id):
    """Processa a exclusão de uma categoria, impedindo exclusão de fixas."""
    if 'user_assinatura_id' not in session:
        flash('Sessão expirada.', 'warning'); return redirect(url_for('auth.login')) # Atualizado

    user_schema = session.get('user_schema')
    redirect_url = url_for('categorias')
    if not user_schema:
        flash('Erro interno.', 'danger'); session.clear(); return redirect(url_for('auth.login')) # Atualizado

    conn = get_db_connection()
    if not conn:
        flash('Erro de conexão.', 'danger'); return redirect(redirect_url)

    cur = None
    try:
        cur = conn.cursor(cursor_factory=DictCursor)

        # Verifica se é fixa
        check_fixa_query = sql.SQL("SELECT nome, is_fixa FROM {schema}.categorias WHERE id = %s").format(schema=sql.Identifier(user_schema))
        cur.execute(check_fixa_query, (categoria_id,))
        categoria_info = cur.fetchone()

        if not categoria_info:
             flash('Categoria não encontrada.', 'warning')
             return redirect(redirect_url)

        if categoria_info['is_fixa']:
            flash('Erro: Categorias pré-definidas não podem ser excluídas.', 'danger')
            return redirect(redirect_url)

        # Verifica uso (se não for fixa)
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

        # Exclui (se não for fixa e não estiver em uso)
        delete_query = sql.SQL("DELETE FROM {schema}.categorias WHERE id = %s AND is_fixa = FALSE").format(schema=sql.Identifier(user_schema))
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
        return redirect(url_for('auth.login')) # Atualizado

    user_schema = session.get('user_schema')
    redirect_url = url_for('lembretes') # Redireciona de volta para a lista de lembretes

    if not user_schema:
        flash('Erro interno: Schema do usuário não encontrado.', 'danger')
        session.clear()
        return redirect(url_for('auth.login')) # Atualizado

    # Obter dados do formulário
    lembrete_id = request.form.get('lembrete_id') # ID virá do input hidden se for edição
    descricao = request.form.get('descricao_lembrete')
    data_str = request.form.get('data_lembrete')

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
                SET descripcion = %s, data = %s, repetir = %s, tipo_repeticion = %s
                WHERE id = %s
            """).format(schema=sql.Identifier(user_schema))
            # Parâmetros na ordem correta: descricao, data, repetir, tipo_rep, id
            query_params = (descricao, data_obj, repetir, tipo_rep, lembrete_id_int)

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
                INSERT INTO {schema}.lembretes (descripcion, data, repetir, tipo_repeticion)
                VALUES (%s, %s, %s, %s)
            """).format(schema=sql.Identifier(user_schema))
            # Parâmetros na ordem correta: descricao, data, repetir, tipo_rep
            query_params = (descricao, data_obj, repetir, tipo_rep)

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
        return redirect(url_for('auth.login')) # Atualizado

    # 2. Obtém o schema do usuário da sessão
    user_schema = session.get('user_schema')
    redirect_url = url_for('receitas') # URL para redirecionar após a exclusão

    # 3. Valida se o schema existe
    if not user_schema:
        flash('Erro interno: Schema do usuário não encontrado.', 'danger')
        session.clear() # Limpa a sessão inválida
        return redirect(url_for('auth.login')) # Atualizado

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
        return redirect(url_for('auth.login')) # Atualizado

    user_schema = session.get('user_schema')
    redirect_url = url_for('lembretes') # Redireciona de volta para a lista

    if not user_schema:
        flash('Erro interno: Schema do usuário não encontrado.', 'danger')
        session.clear()
        return redirect(url_for('auth.login')) # Atualizado

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
    """
    # Verifica se o usuário está logado
    if 'user_assinatura_id' not in session:
        flash('Sessão expirada. Por favor, faça login novamente.', 'warning')
        return redirect(url_for('auth.login')) # Atualizado

    user_schema = session.get('user_schema')
    # URL para redirecionar após a ação (volta para a lista correta)
    redirect_url = url_for('gastos', tipo=tipo_gasto)

    # Verifica se o schema do usuário existe na sessão
    if not user_schema:
        flash('Erro interno: Schema do usuário não encontrado.', 'danger')
        session.clear() # Limpa a sessão inválida
        return redirect(url_for('auth.login')) # Atualizado

    logging.info(f"Tentativa de editar gasto ID {item_id} (Tipo: {tipo_gasto}) no schema {user_schema}")

    # 1. Determina a tabela e os campos com base no tipo de gasto
    if tipo_gasto == 'variaveis':
        table_name = sql.Identifier('gastos')
        # Pega os dados do formulário (nomes dos campos como no HTML)
        descricao_form = request.form.get('descricao')
        valor_str = request.form.get('valor')
        categoria_form = request.form.get('categoria')
        data_str = request.form.get('data') # Campo 'data' para variáveis
        # Dicionário para guardar os valores a serem atualizados no banco
        update_values = {
            'descripcion': descricao_form,
            'valor': None, # Será validado e convertido
            'categoria': categoria_form,
            'data': None # Será validado e convertido
        }
        # Lista de campos que são obrigatórios vir do formulário
        required_fields_check = [descricao_form, valor_str, categoria_form, data_str]
        required_field_names = ['descrição', 'valor', 'categoria', 'data']
        logging.debug(f"Editando Gasto Variável ID {item_id}: Dados recebidos {request.form}")

    elif tipo_gasto == 'fixos':
        table_name = sql.Identifier('gastos_fixos')
        # Pega os dados do formulário
        descricao_form = request.form.get('descricao')
        valor_str = request.form.get('valor')
        categoria_form = request.form.get('categoria')
        data_str = request.form.get('fecha_inicio_fixo') # Campo 'fecha_inicio_fixo' para fixos
        recurrencia_form = request.form.get('recurrencia_fixo')
        # Checkbox: request.form.get('activo_fixo') retorna 'on' se marcado, None caso contrário
        activo_form = request.form.get('activo_fixo') == 'on'
        # Dicionário para guardar os valores a serem atualizados
        update_values = {
            'descripcion': descricao_form,
            'valor': None,
            'categoria': categoria_form,
            'fecha_inicio': None, # Nome da coluna no DB
            'recurrencia': None, # Será validado
            'activo': activo_form # Valor booleano direto
        }
        # Lista de campos obrigatórios (ativo não entra aqui, pois desmarcado não vem)
        required_fields_check = [descricao_form, valor_str, categoria_form, data_str, recurrencia_form]
        required_field_names = ['descrição', 'valor', 'categoria', 'data início', 'recorrência']
        logging.debug(f"Editando Gasto Fixo ID {item_id}: Dados recebidos {request.form}")

    else:
        # Se o tipo_gasto na URL não for 'variaveis' nem 'fixos'
        flash('Tipo de gasto inválido.', 'danger')
        logging.warning(f"Tipo de gasto inválido '{tipo_gasto}' na URL para edição do ID {item_id}.")
        return redirect(url_for('gastos')) # Redireciona para a página padrão de gastos

    # 2. Validação dos campos obrigatórios
    # Verifica se algum dos campos essenciais não foi recebido (é None)
    if not all(field is not None for field in required_fields_check):
        # Encontra quais campos estão faltando para a mensagem de erro
        missing = [name for name, field in zip(required_field_names, required_fields_check) if field is None]
        flash(f'Erro: Preencha todos os campos obrigatórios ({", ".join(missing)}).', 'danger')
        logging.warning(f"Edição falhou para ID {item_id} ({tipo_gasto}): campos obrigatórios ausentes: {missing}.")
        # Não usar redirect aqui ainda, pois a conexão com DB não foi aberta
        # Apenas retorna para o template com a mensagem flash
        # (Considerar passar os dados de volta para o template preencher o form novamente)
        # Por simplicidade, vamos redirecionar, mas o ideal seria re-renderizar o template
        return redirect(redirect_url)


    # 3. Validação e conversão do Valor
    try:
        # Tenta converter o valor para Decimal, tratando vírgula e validando se é positivo
        valor_decimal = Decimal(valor_str.replace(',', '.'))
        if valor_decimal <= 0:
            raise ValueError("O valor do gasto deve ser um número positivo.")
        update_values['valor'] = valor_decimal # Armazena o valor Decimal validado
    except (InvalidOperation, ValueError) as e:
        flash(f'Erro: Valor monetário inválido ({valor_str}). {e}', 'danger')
        logging.warning(f"Edição falhou para ID {item_id} ({tipo_gasto}): valor inválido '{valor_str}'.")
        return redirect(redirect_url)

    # 4. Validação e conversão da Data
    try:
        # Tenta converter a string da data para um objeto date
        data_obj = datetime.strptime(data_str, '%Y-%m-%d').date()
        # Armazena o objeto date no campo correto ('data' ou 'fecha_inicio')
        if tipo_gasto == 'variaveis':
            update_values['data'] = data_obj
        else: # tipo_gasto == 'fixos'
            update_values['fecha_inicio'] = data_obj
    except ValueError:
        flash('Erro: Formato de data inválido. Use AAAA-MM-DD.', 'danger')
        logging.warning(f"Edição falhou para ID {item_id} ({tipo_gasto}): data inválida '{data_str}'.")
        return redirect(redirect_url)

    # 5. Validação da Recorrência (apenas para gastos fixos)
    if tipo_gasto == 'fixos':
        valid_recurrencias = ['mensal', 'bimestral', 'trimestral', 'semestral', 'anual', 'unico']
        # Verifica se a recorrência recebida é uma das válidas
        if not recurrencia_form or recurrencia_form.lower() not in valid_recurrencias:
             flash('Erro: Tipo de recorrência selecionada é inválida.', 'danger')
             logging.warning(f"Edição falhou para ID {item_id} (fixo): recorrência inválida '{recurrencia_form}'.")
             return redirect(redirect_url)
        # Armazena a recorrência validada (em minúsculas)
        update_values['recurrencia'] = recurrencia_form.lower()

    # 6. Conexão com o Banco de Dados e Execução do UPDATE
    conn = get_db_connection()
    if not conn:
        flash('Erro: Falha ao conectar com o banco de dados.', 'danger')
        return redirect(redirect_url)

    cur = None
    try:
        cur = conn.cursor()
        # Monta a cláusula SET da query SQL dinamicamente
        set_clauses = []
        query_params = []
        # Itera sobre os valores que preparamos para atualização
        for col, val in update_values.items():
            # Inclui a coluna no SET apenas se tivermos um valor válido para ela
            # (None foi usado como placeholder para valores ainda não validados/convertidos)
            # O campo 'activo' é um caso especial, pois False é um valor válido.
            if val is not None or col == 'activo':
                 # Cria a parte "nome_coluna = %s" de forma segura
                 set_clauses.append(sql.SQL("{} = %s").format(sql.Identifier(col)))
                 # Adiciona o valor correspondente à lista de parâmetros
                 query_params.append(val)

        # Se, por algum motivo, não houver cláusulas SET válidas, informa o usuário
        if not set_clauses:
             flash('Nenhum dado válido fornecido para atualização.', 'warning')
             logging.warning(f"Edição para ID {item_id} ({tipo_gasto}): Nenhum campo válido para incluir na query UPDATE.")
             # Não precisa redirecionar aqui, pois o finally fechará a conexão
        else:
            # Adiciona o ID do item ao final da lista de parâmetros para a cláusula WHERE
            query_params.append(item_id)

            # Constrói a query UPDATE final de forma segura
            update_query = sql.SQL("UPDATE {schema}.{table} SET {set_sql} WHERE id = %s").format(
                schema=sql.Identifier(user_schema), # Schema do usuário
                table=table_name,                   # Tabela correta (gastos ou gastos_fixos)
                set_sql=sql.SQL(', ').join(set_clauses) # Junta as cláusulas SET com vírgula
            )

            logging.debug(f"Executando UPDATE: Query={update_query.as_string(conn)} Params={query_params}")
            # Executa a query
            cur.execute(update_query, query_params)
            # Confirma a transação no banco de dados
            conn.commit()

            # Verifica se alguma linha foi afetada pela atualização
            if cur.rowcount > 0:
                flash('¡Gasto actualizado con éxito!', 'success')
                logging.info(f"Gasto ID {item_id} (Tipo: {tipo_gasto}) atualizado com sucesso no schema {user_schema}.")
            else:
                # Se rowcount for 0, o ID não foi encontrado ou os dados eram idênticos
                flash('Gasto no encontrado o ningún dato fue modificado.', 'warning')
                logging.warning(f"Edição para ID {item_id} ({tipo_gasto}): rowcount foi 0 (item não encontrado ou dados idênticos).")

    except psycopg2.Error as e:
        # Em caso de erro do psycopg2, desfaz a transação e loga o erro
        if conn: conn.rollback()
        flash(f'Error: {e}', 'danger')
        logging.error(f"Erro DB (psycopg2) ao editar gasto {tipo_gasto} ID {item_id} schema {user_schema}: {e}")
    except Exception as e:
        # Em caso de outro erro Python, desfaz a transação e loga
        if conn: conn.rollback()
        flash(f'Error', 'danger')
        logging.error(f"Erro inesperado (Python) ao editar gasto {tipo_gasto} ID {item_id} schema {user_schema}: {e}", exc_info=True)
    finally:
        # Garante que o cursor e a conexão sejam fechados
        if cur: cur.close()
        if conn: conn.close()

    # Redireciona o usuário de volta para a lista de gastos
    return redirect(redirect_url)


@app.route('/gastos/<string:tipo_gasto>/<int:item_id>/delete', methods=['POST'])
def delete_gasto(tipo_gasto, item_id):
    """
    Processa a solicitação de exclusão de um gasto (variável ou fixo).
    """
    # Verifica se o usuário está logado
    if 'user_assinatura_id' not in session:
        flash('Sessão expirada. Por favor, faça login novamente.', 'warning')
        return redirect(url_for('auth.login')) # Atualizado

    user_schema = session.get('user_schema')
    # URL para redirecionar após a ação
    redirect_url = url_for('gastos', tipo=tipo_gasto)

    # Verifica o schema
    if not user_schema:
        flash('Erro interno: Schema do usuário não encontrado.', 'danger')
        session.clear()
        return redirect(url_for('auth.login')) # Atualizado

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
def dashboard():
    if 'user_assinatura_id' not in session:
        flash('Você precisa fazer login para acessar esta página.', 'warning')
        return redirect(url_for('auth.login')) # Atualizado

    user_schema = session.get('user_schema')
    user_nome = session.get('user_nome', session.get('user_email'))

    if not user_schema:
        logging.error(f"Schema não encontrado na sessão para usuário {user_nome} (ID: {session.get('user_assinatura_id')}) ao acessar /dashboard. Deslogando.")
        flash('Erro interno: Informações do usuário incompletas. Por favor, faça login novamente.', 'danger')
        session.clear()
        return redirect(url_for('auth.login')) # Atualizado

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
        "total_receitas_mes": Decimal('0.00'), # Manteremos nome da chave para o HTML
        "total_despesas_mes": Decimal('0.00'),# Manteremos nome da chave para o HTML
        "saldo_mes": Decimal('0.00'),         # Manteremos nome da chave para o HTML
        "movimentacoes_recentes": [],
        "proximos_lembretes": [],
        "gastos_categoria_labels": [],
        "gastos_categoria_data": [],
        "gastos_tempo_labels": [],
        "gastos_tempo_data": []
    }
    meta_ativa = None
    categorias_por_tipo = {
        'receita': [],
        'gasto_variavel': [],
        'gasto_fixo': []
    }

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

        # --- Buscar Dados Financeiros para o PERÍODO SELECIONADO ---

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
        movimentacoes.sort(key=lambda x: x['data'], reverse=True)
        dados['movimentacoes_recentes'] = movimentacoes[:5]

        # --- Buscar Próximos Lembretes (Não depende do período do card) ---
        query_lembretes_dashboard = sql.SQL(
            "SELECT id, descripcion, data FROM {schema}.lembretes "
            "WHERE data >= CURRENT_DATE ORDER BY data ASC LIMIT 5"
        ).format(schema=sql.Identifier(user_schema))
        cur.execute(query_lembretes_dashboard)
        dados['proximos_lembretes'] = [dict(l_rem) for l_rem in cur.fetchall()]

        # --- Buscar Dados para Gráficos ---
        # Gráfico de Gastos por Categoria (MANTIDO MENSAL POR SIMPLICIDADE INICIAL)
        query_gastos_cat_chart = sql.SQL(
            "SELECT categoria, SUM(valor) as total FROM {schema}.gastos " # APENAS GASTOS VARIÁVEIS
            "WHERE EXTRACT(MONTH FROM data) = EXTRACT(MONTH FROM CURRENT_DATE) "
            "AND EXTRACT(YEAR FROM data) = EXTRACT(YEAR FROM CURRENT_DATE) "
            "AND categoria IS NOT NULL GROUP BY categoria ORDER BY total DESC"
        ).format(schema=sql.Identifier(user_schema))
        cur.execute(query_gastos_cat_chart)
        gastos_por_categoria_chart = cur.fetchall()
        dados['gastos_categoria_labels'] = [g_cat['categoria'] for g_cat in gastos_por_categoria_chart]
        dados['gastos_categoria_data'] = [g_cat['total'] for g_cat in gastos_por_categoria_chart]

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

        # --- Buscar Meta Ativa (Não depende do período do card) ---
        query_meta_ativa = sql.SQL(
            "SELECT * FROM {schema}.metas WHERE status = 'ativa' ORDER BY id DESC LIMIT 1"
        ).format(schema=sql.Identifier(user_schema))
        cur.execute(query_meta_ativa)
        meta_ativa = cur.fetchone()

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
        "gastos_tempo_labels": dados['gastos_tempo_labels'],
        "gastos_tempo_data": dados['gastos_tempo_data']
    }, default=json_converter)

    return render_template('dashboard.html',
                           user_nome=user_nome,
                           dados=dados,
                           meta_ativa=meta_ativa,
                           dados_json=dados_json_string,
                           categorias_por_tipo=categorias_por_tipo,
                           periodo_ativo=periodo_selecionado) # Passa o período ativo para o template




# A rota /logout foi movida para auth.py

# --- Rotas de Gastos (Variáveis e Fixos) ---


@app.route('/gastos', methods=['GET'])
def gastos():
    if 'user_assinatura_id' not in session:
        flash('Inicio de sesión necesario.', 'warning')
        return redirect(url_for('auth.login')) # Atualizado

    user_schema = session.get('user_schema')
    user_nome = session.get('user_nome', session.get('user_email'))
    if not user_schema:
        flash('Erro interno.', 'danger')
        session.clear()
        return redirect(url_for('auth.login')) # Atualizado

    # --- Obter Parâmetros da Requisição (Filtros, Ordenação, Página) ---
    tipo_gasto_ativo = request.args.get('tipo', 'variaveis').lower()
    data_inicio_str = request.args.get('data_inicio')
    data_fim_str = request.args.get('data_fim')
    categoria_filtro = request.args.get('categoria_filtro', 'todas')
    filtro_activo = request.args.get('filtro_activo') # Para gastos fixos
    default_sort_by = 'data_desc' if tipo_gasto_ativo == 'variaveis' else 'fecha_inicio_desc'
    sort_by = request.args.get('sort_by', default_sort_by)
    page = request.args.get('page', 1, type=int) # Obtém o número da página, padrão é 1

    # --- Inicialização ---
    lista_itens = []
    categorias_disponiveis = []
    categorias_disponiveis_add_edit = []
    total_items = 0
    total_pages = 1
    current_page = page

    conn = get_db_connection()
    if not conn:
        flash('Erro conexão DB.', 'danger')
        # Retorna template com valores vazios/padrão em caso de erro de conexão
        return render_template('gastos.html', user_nome=user_nome, itens=lista_itens,
                               categorias_disponiveis=categorias_disponiveis,
                               categorias_disponiveis_add_edit=categorias_disponiveis_add_edit,
                               filtros_aplicados={}, tipo_gasto_ativo=tipo_gasto_ativo,
                               current_page=1, total_pages=1, items_per_page=ITEMS_PER_PAGE)

    cur = None
    try:
        cur = conn.cursor(cursor_factory=DictCursor)

        # --- Determinar Tabela e Colunas Base ---
        if tipo_gasto_ativo == 'fixos':
            table_name = sql.Identifier('gastos_fixos')
            select_columns = sql.SQL("id, fecha_inicio as data, descripcion, valor, categoria, recurrencia, activo")
            date_column_filter = sql.Identifier('fecha_inicio')
            default_order_column = sql.Identifier('fecha_inicio')
            query_categorias = sql.SQL("SELECT DISTINCT categoria FROM {schema}.{table} WHERE categoria IS NOT NULL ORDER BY categoria ASC").format(schema=sql.Identifier(user_schema), table=table_name)
            categorias_disponiveis_add_edit = buscar_categorias_por_tipo(conn, user_schema, 'gasto_fixo')
        else: # 'variaveis'
            table_name = sql.Identifier('gastos')
            select_columns = sql.SQL("id, data, descripcion, valor, categoria")
            date_column_filter = sql.Identifier('data')
            default_order_column = sql.Identifier('data')
            query_categorias = sql.SQL("SELECT DISTINCT categoria FROM {schema}.{table} WHERE categoria IS NOT NULL ORDER BY categoria ASC").format(schema=sql.Identifier(user_schema), table=table_name)
            categorias_disponiveis_add_edit = buscar_categorias_por_tipo(conn, user_schema, 'gasto_variavel')

        # Buscar categorias disponíveis para o filtro
        cur.execute(query_categorias)
        categorias_disponiveis = [row['categoria'] for row in cur.fetchall()]

        # --- Construir Cláusula WHERE para Filtros ---
        where_clauses = []
        query_params = [] # Lista de parâmetros para a query principal

        # Adiciona filtro de data_inicio
        if data_inicio_str:
            try:
                datetime.strptime(data_inicio_str, '%Y-%m-%d')
                where_clauses.append(sql.SQL("{date_col} >= %s").format(date_col=date_column_filter))
                query_params.append(data_inicio_str)
            except ValueError:
                flash('Data início inválida.', 'warning')
                data_inicio_str = None # Invalida para filtros_aplicados

        # Adiciona filtro de data_fim
        if data_fim_str:
            try:
                datetime.strptime(data_fim_str, '%Y-%m-%d')
                where_clauses.append(sql.SQL("{date_col} <= %s").format(date_col=date_column_filter))
                query_params.append(data_fim_str)
            except ValueError:
                flash('Data fim inválida.', 'warning')
                data_fim_str = None # Invalida para filtros_aplicados

        # Adiciona filtro de categoria
        if categoria_filtro and categoria_filtro != 'todas':
            where_clauses.append(sql.SQL("categoria = %s"))
            query_params.append(categoria_filtro)

        # Adiciona filtro de ativo (apenas para gastos fixos)
        if tipo_gasto_ativo == 'fixos' and filtro_activo:
            if filtro_activo == 'true':
                where_clauses.append(sql.SQL("activo = TRUE"))
            elif filtro_activo == 'false':
                where_clauses.append(sql.SQL("activo = FALSE"))

        # Monta a parte WHERE da query
        where_sql = sql.SQL(" WHERE ") + sql.SQL(" AND ").join(where_clauses) if where_clauses else sql.SQL("")

        # --- Calcular Total de Itens e Páginas (NOVO) ---
        count_query = sql.SQL("SELECT COUNT(*) FROM {schema}.{table} {where}").format(
            schema=sql.Identifier(user_schema),
            table=table_name,
            where=where_sql
        )
        # Executa a contagem com os MESMOS parâmetros de filtro
        cur.execute(count_query, query_params)
        total_items = cur.fetchone()[0]
        total_pages = ceil(total_items / ITEMS_PER_PAGE) if total_items > 0 else 1
        # Garante que a página atual não seja maior que o total de páginas
        current_page = min(page, total_pages)
        # Calcula o OFFSET para a query principal
        offset = (current_page - 1) * ITEMS_PER_PAGE

        # --- Construir Cláusula ORDER BY ---
        order_by_options = {
            'data_desc': sql.SQL(" ORDER BY {col} DESC, id DESC").format(col=default_order_column),
            'data_asc': sql.SQL(" ORDER BY {col} ASC, id ASC").format(col=default_order_column),
            'valor_desc': sql.SQL(" ORDER BY valor DESC, {col} DESC, id DESC").format(col=default_order_column),
            'valor_asc': sql.SQL(" ORDER BY valor ASC, {col} DESC, id DESC").format(col=default_order_column),
            'categoria_asc': sql.SQL(" ORDER BY categoria ASC, {col} DESC, id DESC").format(col=default_order_column),
            'id_desc': sql.SQL(" ORDER BY id DESC"),
            'id_asc': sql.SQL(" ORDER BY id ASC")
        }
        if tipo_gasto_ativo == 'fixos':
            order_by_options['fecha_inicio_desc'] = sql.SQL(" ORDER BY fecha_inicio DESC, id DESC")
            order_by_options['fecha_inicio_asc'] = sql.SQL(" ORDER BY fecha_inicio ASC, id ASC")
            order_by_options['recurrencia_asc'] = sql.SQL(" ORDER BY recurrencia ASC, fecha_inicio DESC, id DESC")
            # Adiciona ordenação por 'activo' se necessário
            order_by_options['activo_desc'] = sql.SQL(" ORDER BY activo DESC, {col} DESC, id DESC").format(col=default_order_column) # Ativos primeiro
            order_by_options['activo_asc'] = sql.SQL(" ORDER BY activo ASC, {col} DESC, id DESC").format(col=default_order_column) # Inativos primeiro

        # Usa a ordenação padrão apropriada se a opção for inválida
        order_by_clause = order_by_options.get(sort_by, order_by_options.get(default_sort_by))

        # --- Construir Query Principal com LIMIT e OFFSET (NOVO) ---
        limit_offset_sql = sql.SQL(" LIMIT %s OFFSET %s")
        query_params_main = query_params + [ITEMS_PER_PAGE, offset] # Adiciona LIMIT e OFFSET aos parâmetros

        final_query_sql = sql.SQL("SELECT {columns} FROM {schema}.{table} {where} {order} {limit_offset}").format(
            columns=select_columns,
            schema=sql.Identifier(user_schema),
            table=table_name,
            where=where_sql,
            order=order_by_clause,
            limit_offset=limit_offset_sql
        )

        # --- Executar Query Principal ---
        logging.debug(f"Executando Query Gastos (Pag {current_page}): {final_query_sql.as_string(conn)} Params: {query_params_main}")
        cur.execute(final_query_sql, query_params_main)
        lista_itens = cur.fetchall()
        logging.info(f"Buscados {len(lista_itens)} itens ({tipo_gasto_ativo}) para página {current_page}/{total_pages} (Total: {total_items})")

    except psycopg2.Error as e:
        flash(f'Erro DB ao buscar gastos ({tipo_gasto_ativo}).', 'danger')
        logging.error(f"Erro DB /gastos {user_schema}, Tipo: {tipo_gasto_ativo}, Página: {page}: {e}")
        # Resetar valores para evitar erros no template
        lista_itens = []
        total_items = 0
        total_pages = 1
        current_page = 1
    except Exception as e:
        flash(f'Erro inesperado ao buscar gastos ({tipo_gasto_ativo}).', 'danger')
        logging.error(f"Erro inesperado /gastos {user_schema}, Tipo: {tipo_gasto_ativo}, Página: {page}: {e}", exc_info=True)
        lista_itens = []
        total_items = 0
        total_pages = 1
        current_page = 1
    finally:
        if cur: cur.close()
        if conn: conn.close()

    # Guarda os filtros aplicados para usar no template (preencher modais, etc.)
    filtros_aplicados = {
        'data_inicio': data_inicio_str, 'data_fim': data_fim_str,
        'categoria_filtro': categoria_filtro, 'sort_by': sort_by,
        'tipo_gasto_ativo': tipo_gasto_ativo,
        'filtro_activo': filtro_activo if tipo_gasto_ativo == 'fixos' else None
    }

    # Renderiza o template passando os dados da PÁGINA ATUAL e informações de paginação
    return render_template('gastos.html',
                           user_nome=user_nome,
                           itens=lista_itens, # Itens apenas da página atual
                           categorias_disponiveis=categorias_disponiveis,
                           categorias_disponiveis_add_edit=categorias_disponiveis_add_edit,
                           filtros_aplicados=filtros_aplicados,
                           tipo_gasto_ativo=tipo_gasto_ativo,
                           current_page=current_page, # Número da página atual
                           total_pages=total_pages,   # Número total de páginas
                           items_per_page=ITEMS_PER_PAGE) # Itens por página (opcional, mas útil)

@app.route('/add_gasto', methods=['POST'])
def add_gasto():
    # Verifica sessão
    if 'user_assinatura_id' not in session:
        flash('Sessão expirada.', 'warning')
        return redirect(url_for('auth.login')) # Atualizado

    user_schema = session.get('user_schema')
    # Define URL de redirecionamento (pode vir do dashboard ou da página de gastos)
    redirect_url = request.referrer or url_for('gastos', tipo='variaveis')

    # Verifica schema
    if not user_schema:
        flash('Erro interno.', 'danger')
        session.clear()
        return redirect(url_for('auth.login')) # Atualizado

    # Obtém dados do formulário
    descricao_form = request.form.get('descricao')
    valor_str = request.form.get('valor')
    categoria = request.form.get('categoria')
    data_gasto_str = request.form.get('data')

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

        # Se a categoria é válida, prossegue com a inserção
        cur = conn.cursor()
        insert_query = sql.SQL("INSERT INTO {}.gastos (descripcion, valor, categoria, data) VALUES (%s, %s, %s, %s)").format(
            sql.Identifier(user_schema)
        )
        cur.execute(insert_query, (descricao_form, valor_decimal, categoria, data_gasto_obj))
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
        return redirect(url_for('auth.login')) # Atualizado

    user_schema = session.get('user_schema')
    # Define URL de redirecionamento
    redirect_url = request.referrer or url_for('gastos', tipo='fixos')

    # Verifica schema
    if not user_schema:
        flash('Erro interno.', 'danger')
        session.clear()
        return redirect(url_for('auth.login')) # Atualizado

    # Obtém dados do formulário
    descricao = request.form.get('descricao_fixo')
    valor_str = request.form.get('valor_fixo')
    categoria = request.form.get('categoria_fixo')
    fecha_inicio_str = request.form.get('fecha_inicio_fixo')
    recurrencia = request.form.get('recurrencia_fixo')
    activo = request.form.get('activo_fixo') == 'on' # Checkbox retorna 'on' se marcado

    # Validação básica dos campos
    if not all([descricao, valor_str, categoria, fecha_inicio_str, recurrencia]):
        flash('Campos obrigatórios: Descrição, Valor, Categoria, Data Início e Recorrência.', 'danger')
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

    # Validação da recorrência
    valid_recurrencias = ['mensal', 'bimestral', 'trimestral', 'semestral', 'anual', 'unico']
    if not recurrencia or recurrencia.lower() not in valid_recurrencias:
        flash('Tipo de recorrência inválido.', 'danger')
        return redirect(redirect_url)
    recurrencia_lower = recurrencia.lower() # Garante minúsculas para salvar

    # Conexão com o banco
    conn = get_db_connection()
    if not conn:
        flash('Erro de conexão com o banco de dados.', 'danger')
        return redirect(redirect_url)

    cur = None
    try:
        # *** Validação da Categoria ***
        if not validar_categoria(conn, user_schema, categoria, 'gasto_fixo'):
            flash(f'Erro: Categoria "{categoria}" inválida ou não permitida para Gasto Fixo.', 'danger')
            logging.warning(f"Tentativa de adicionar gasto fixo com categoria inválida '{categoria}' no schema {user_schema}")
            return redirect(redirect_url)

        # Se a categoria é válida, prossegue com a inserção
        cur = conn.cursor()
        insert_query = sql.SQL("""
            INSERT INTO {schema}.gastos_fixos
            (descripcion, valor, categoria, fecha_inicio, recurrencia, activo)
            VALUES (%s, %s, %s, %s, %s, %s)
        """).format(schema=sql.Identifier(user_schema))
        cur.execute(insert_query, (descricao, valor_decimal, categoria, fecha_inicio_obj, recurrencia_lower, activo))
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


# --- Rotas de Lembretes ---
@app.route('/lembretes')
def lembretes():
    if 'user_assinatura_id' not in session: flash('Login necessário.', 'warning'); return redirect(url_for('auth.login')) # Atualizado
    user_schema = session.get('user_schema'); user_nome = session.get('user_nome', session.get('user_email'))
    if not user_schema: flash('Erro interno.', 'danger'); session.clear(); return redirect(url_for('auth.login')) # Atualizado
    lista_de_lembretes = []
    conn = get_db_connection()
    if conn:
        cur = None
        try: cur = conn.cursor(cursor_factory=DictCursor); query = sql.SQL("SELECT id, descripcion, data, repetir, tipo_repeticion FROM {}.lembretes ORDER BY data ASC, id ASC").format(sql.Identifier(user_schema)); cur.execute(query); lista_de_lembretes = cur.fetchall()
        except psycopg2.Error as e: flash('Erro DB lembretes.', 'danger'); logging.error(f"Erro DB /lembretes {user_schema}: {e}"); lista_de_lembretes = []
        finally:
            if cur: cur.close()
            if conn: conn.close()
    else: flash('Erro conexão DB.', 'danger'); lista_de_lembretes = []
    data_hoje = date.today()
    return render_template('lembretes.html', user_nome=user_nome, lembretes=lista_de_lembretes, data_hoje=data_hoje)

@app.route('/add_lembrete_from_modal', methods=['POST'])
def add_lembrete_from_modal():
    # 1. Verifica sessão
    if 'user_assinatura_id' not in session:
        flash('Sessão expirada.', 'warning')
        return redirect(url_for('auth.login')) # Atualizado

    # 2. Obtém schema e URL de redirecionamento
    user_schema = session.get('user_schema')
    referer_url = request.referrer
    # Decide se volta para lembretes ou dashboard
    redirect_url = url_for('lembretes') if referer_url and '/lembretes' in referer_url else url_for('dashboard')

    if not user_schema:
        flash('Erro interno.', 'danger')
        session.clear()
        return redirect(url_for('auth.login')) # Atualizado

    # --- 3. Obter e Validar Dados do Formulário ---
    descricao = request.form.get('descricao_lembrete')
    data_lembrete_str = request.form.get('data_lembrete')

    # Validação básica
    if not descricao or not data_lembrete_str:
        flash('Descrição e data obrigatórias.', 'danger')
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
        # Query INSERT atualizada para incluir as colunas repetir e tipo_repeticion
        query = sql.SQL("""
            INSERT INTO {schema}.lembretes (descripcion, data, repetir, tipo_repeticion)
            VALUES (%s, %s, %s, %s)
        """).format(schema=sql.Identifier(user_schema))
        # Parâmetros agora incluem os valores booleanos e de string para repetição
        params = (descricao, data_lembrete_obj, repetir, tipo_rep)

        logging.debug(f"Executando INSERT Lembrete (Modal): Query={query.as_string(conn)} Params={params}")
        cur.execute(query, params)
        conn.commit()
        flash('Lembrete adicionado!', 'success')
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


@app.route('/receitas', methods=['GET']) # MODIFICADO: Removido 'POST'
def receitas():
    # Verifica login
    if 'user_assinatura_id' not in session:
        flash('Você precisa fazer login para acessar esta página.', 'warning')
        return redirect(url_for('auth.login')) # Atualizado

    user_schema = session.get('user_schema')
    user_nome = session.get('user_nome', session.get('user_email'))

    # Verifica schema
    if not user_schema:
        flash('Erro interno: Informações do usuário incompletas.', 'danger')
        session.clear()
        return redirect(url_for('auth.login')) # Atualizado

    conn = get_db_connection()
    if not conn:
        flash('Erro de conexão com o banco de dados.', 'danger')
        # Valores padrão em caso de falha de conexão
        return render_template('receitas.html', user_nome=user_nome,
                               # salario_principal=Decimal('0.00'), # REMOVIDO
                               outras_receitas=[], total_outras_receitas_mes=Decimal('0.00'),
                               total_entradas_mes=Decimal('0.00'), categorias_disponiveis=[],
                               categorias_receitas_formulario=[], filtros_aplicados={},
                               current_page=1, total_pages=1)

    cur = None
    try:
        # --- Tratamento do POST FOI REMOVIDO ---

        # --- Tratamento do GET (Exibição da Página) ---
        # Inicialização das variáveis
        # salario_principal_atual = Decimal('0.00') # REMOVIDO
        lista_outras_receitas = []
        total_outras_receitas_mes_atual = Decimal('0.00')
        # total_entradas_mes_atual = Decimal('0.00') # Será calculado abaixo
        categorias_disponiveis = []
        categorias_receitas_formulario = []
        total_items = 0
        total_pages = 1

        data_inicio_str = request.args.get('data_inicio')
        data_fim_str = request.args.get('data_fim')
        categoria_filtro = request.args.get('categoria_filtro', 'todas')
        sort_by = request.args.get('sort_by', 'fecha_desc')
        page = request.args.get('page', 1, type=int)
        current_page = page

        data_inicio_obj = None; data_fim_obj = None
        if data_inicio_str:
            try: data_inicio_obj = datetime.strptime(data_inicio_str, '%Y-%m-%d').date()
            except ValueError: flash('Data início inválida.', 'warning'); data_inicio_str = None
        if data_fim_str:
            try: data_fim_obj = datetime.strptime(data_fim_str, '%Y-%m-%d').date()
            except ValueError: flash('Data fim inválida.', 'warning'); data_fim_str = None
        if data_inicio_obj and data_fim_obj and data_fim_obj < data_inicio_obj:
            flash('Data fim anterior a início.', 'warning'); data_inicio_obj = None; data_fim_obj = None; data_inicio_str = None; data_fim_str = None

        filtros_aplicados = {
            'data_inicio': data_inicio_str,
            'data_fim': data_fim_str,
            'categoria_filtro': categoria_filtro,
            'sort_by': sort_by
        }

        cur = conn.cursor(cursor_factory=DictCursor)

        # Busca salário principal FOI REMOVIDA

        # Soma outras receitas do mês atual (para o resumo no topo da página)
        query_soma_outras_receitas = sql.SQL("""
            SELECT COALESCE(SUM(valor), 0) as total_outras_receitas 
            FROM {schema}.outras_receitas 
            WHERE EXTRACT(MONTH FROM fecha) = EXTRACT(MONTH FROM CURRENT_DATE) 
            AND EXTRACT(YEAR FROM fecha) = EXTRACT(YEAR FROM CURRENT_DATE)
        """).format(schema=sql.Identifier(user_schema))
        cur.execute(query_soma_outras_receitas)
        outras_receitas_result = cur.fetchone()
        if outras_receitas_result and outras_receitas_result['total_outras_receitas'] is not None:
            total_outras_receitas_mes_atual = outras_receitas_result['total_outras_receitas']
        
        total_entradas_mes_atual = total_outras_receitas_mes_atual # MODIFICADO: Agora é só isso

        query_categorias = sql.SQL("SELECT DISTINCT categoria FROM {schema}.outras_receitas WHERE categoria IS NOT NULL ORDER BY categoria ASC").format(schema=sql.Identifier(user_schema))
        cur.execute(query_categorias)
        categorias_disponiveis = [row['categoria'] for row in cur.fetchall()]

        categorias_receitas_formulario = buscar_categorias_por_tipo(conn, user_schema, 'receita')

        where_clauses = []
        query_params = []
        if data_inicio_obj: where_clauses.append(sql.SQL("fecha >= %s")); query_params.append(data_inicio_obj)
        if data_fim_obj: where_clauses.append(sql.SQL("fecha <= %s")); query_params.append(data_fim_obj)
        if categoria_filtro and categoria_filtro != 'todas': where_clauses.append(sql.SQL("categoria = %s")); query_params.append(categoria_filtro)
        where_sql = sql.SQL(" WHERE ") + sql.SQL(" AND ").join(where_clauses) if where_clauses else sql.SQL("")

        count_query = sql.SQL("SELECT COUNT(*) FROM {schema}.outras_receitas {where}").format(
            schema=sql.Identifier(user_schema),
            where=where_sql
        )
        cur.execute(count_query, query_params)
        total_items = cur.fetchone()[0]
        total_pages = ceil(total_items / ITEMS_PER_PAGE) if total_items > 0 else 1
        current_page = min(page, total_pages) if total_pages > 0 else 1 # Garante que current_page seja ao menos 1
        offset = (current_page - 1) * ITEMS_PER_PAGE

        order_by_options = {
            'fecha_desc': sql.SQL(" ORDER BY fecha DESC, id DESC"),
            'fecha_asc': sql.SQL(" ORDER BY fecha ASC, id ASC"),
            'valor_desc': sql.SQL(" ORDER BY valor DESC, fecha DESC, id DESC"),
            'valor_asc': sql.SQL(" ORDER BY valor ASC, fecha DESC, id DESC"),
            'categoria_asc': sql.SQL(" ORDER BY categoria ASC, fecha DESC, id DESC"),
            'id_desc': sql.SQL(" ORDER BY id DESC"),
            'id_asc': sql.SQL(" ORDER BY id ASC")
        }
        order_by_clause = order_by_options.get(sort_by, order_by_options['fecha_desc'])

        base_query = sql.SQL("SELECT id, fecha, categoria, descripcion, valor FROM {schema}.outras_receitas").format(schema=sql.Identifier(user_schema))
        limit_offset_sql = sql.SQL(" LIMIT %s OFFSET %s")
        query_params_main = query_params + [ITEMS_PER_PAGE, offset]

        final_query_sql = base_query + where_sql + order_by_clause + limit_offset_sql

        logging.debug(f"Executando Query Receitas (Pag {current_page}): {final_query_sql.as_string(conn)} Params: {query_params_main}")
        cur.execute(final_query_sql, query_params_main)
        lista_outras_receitas = cur.fetchall()
        logging.info(f"Buscados {len(lista_outras_receitas)} itens (receitas) para página {current_page}/{total_pages} (Total: {total_items}). Filtros: {filtros_aplicados}")

        return render_template('receitas.html',
                               user_nome=user_nome,
                               # salario_principal=salario_principal_atual, # REMOVIDO
                               outras_receitas=lista_outras_receitas,
                               total_outras_receitas_mes=total_outras_receitas_mes_atual,
                               total_entradas_mes=total_entradas_mes_atual,
                               categorias_disponiveis=categorias_disponiveis,
                               categorias_receitas_formulario=categorias_receitas_formulario,
                               filtros_aplicados=filtros_aplicados,
                               current_page=current_page,
                               total_pages=total_pages)

    except psycopg2.Error as e:
        flash('Erro de banco de dados na página de receitas.', 'danger')
        logging.error(f"Erro DB /receitas {user_schema}: {e}")
        return render_template('receitas.html', user_nome=user_nome, # salario_principal=Decimal('0.00'), # REMOVIDO
                               outras_receitas=[], total_outras_receitas_mes=Decimal('0.00'),
                               total_entradas_mes=Decimal('0.00'), categorias_disponiveis=[],
                               categorias_receitas_formulario=[], filtros_aplicados={},
                               current_page=1, total_pages=1)
    except Exception as e:
        flash('Erro inesperado na página de receitas.', 'danger')
        logging.error(f"Erro inesperado /receitas {user_schema}: {e}", exc_info=True)
        return render_template('receitas.html', user_nome=user_nome, # salario_principal=Decimal('0.00'), # REMOVIDO
                               outras_receitas=[], total_outras_receitas_mes=Decimal('0.00'),
                               total_entradas_mes=Decimal('0.00'), categorias_disponiveis=[],
                               categorias_receitas_formulario=[], filtros_aplicados={},
                               current_page=1, total_pages=1)
    finally:
        if cur: cur.close()
        if conn: conn.close()





@app.route('/add_outra_receita', methods=['POST'])
def add_outra_receita():
    # Verifica sessão
    if 'user_assinatura_id' not in session:
        flash('Sessão expirada.', 'warning')
        return redirect(url_for('auth.login')) # Atualizado

    user_schema = session.get('user_schema')
    # Define URL de redirecionamento
    redirect_url = request.referrer or url_for('receitas')

    # Verifica schema
    if not user_schema:
        flash('Erro interno.', 'danger')
        session.clear()
        return redirect(url_for('auth.login')) # Atualizado

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
@app.route('/metas', methods=['POST'])
def metas():
    if 'user_assinatura_id' not in session:
        flash('Você precisa fazer login.', 'warning')
        return redirect(url_for('auth.login')) # Atualizado

    user_schema = session.get('user_schema')
    if not user_schema:
        flash('Erro interno: Schema do usuário não encontrado.', 'danger')
        return redirect(url_for('auth.login')) # Atualizado

    conn = get_db_connection()
    if not conn:
        flash('Erro de conexão com o banco de dados.', 'danger')
        return redirect(url_for('dashboard'))

    cur = None
    try:
        descricao = request.form.get('meta_descricao')
        categoria = request.form.get('meta_categoria')
        prazo_form_value = request.form.get('meta_prazo')
        valor_alvo_str = request.form.get('meta_valor_alvo')

        # CORREÇÃO APLICADA: Separar validação
        if not all([descricao, categoria, prazo_form_value, valor_alvo_str]):
            flash('Erro: Descrição, Categoria, Prazo e Valor Alvo da meta são obrigatórios.', 'danger')
            return redirect(url_for('dashboard'))

        try:
            valor_alvo = Decimal(valor_alvo_str.replace(',', '.'))
            if valor_alvo <= 0:
                raise ValueError("O valor alvo da meta deve ser um número positivo.")
        except (InvalidOperation, ValueError) as e:
            flash(f'Erro: Valor alvo inválido. {e}', 'danger')
            return redirect(url_for('dashboard'))

        prazo_meses = None
        if prazo_form_value and prazo_form_value.isdigit():
            try:
                prazo_meses = int(prazo_form_value)
                if prazo_meses <= 0:
                     flash('Erro: O prazo em meses, se informado, deve ser um número positivo.', 'danger')
                     return redirect(url_for('dashboard'))
            except ValueError:
                 flash('Erro: Prazo em meses inválido.', 'danger')
                 return redirect(url_for('dashboard'))
        elif prazo_form_value != 'indefinido':
            flash('Erro: Valor de prazo inválido.', 'danger')
            return redirect(url_for('dashboard'))

        valid_categorias = ['Viagem', 'Compra', 'Guardar dinheiro', 'Outros']
        if categoria not in valid_categorias:
            flash('Erro: Categoria de meta inválida.', 'danger')
            return redirect(url_for('dashboard'))

        valor_mensal_sugerido = None; data_conclusao_prevista = None; data_inicio = date.today()
        if prazo_meses and valor_alvo > 0:
            try:
                valor_mensal_sugerido = round(valor_alvo / Decimal(prazo_meses), 2)
                data_conclusao_prevista = data_inicio + relativedelta(months=prazo_meses)
            except Exception as e: logging.error(f"Erro cálculo meta {user_schema}: {e}")

        cur = conn.cursor(cursor_factory=DictCursor)
        find_active_query = sql.SQL("SELECT id FROM {}.metas WHERE status = 'ativa' LIMIT 1").format(sql.Identifier(user_schema))
        cur.execute(find_active_query); meta_existente_ativa = cur.fetchone()

        if meta_existente_ativa:
            update_query = sql.SQL("UPDATE {schema}.metas SET descricao = %s, categoria = %s, prazo_meses = %s, valor_alvo = %s, valor_mensal_sugerido = %s, data_inicio = %s, data_conclusao_prevista = %s, valor_atual = valor_atual, status = 'ativa', atualizado_em = NOW() WHERE id = %s").format(schema=sql.Identifier(user_schema))
            cur.execute(update_query, (descricao, categoria, prazo_meses, valor_alvo, valor_mensal_sugerido, data_inicio, data_conclusao_prevista, meta_existente_ativa['id']))
            flash('Meta atualizada!', 'success')
        else:
            insert_query = sql.SQL("INSERT INTO {schema}.metas (descricao, categoria, prazo_meses, valor_alvo, valor_mensal_sugerido, data_inicio, data_conclusao_prevista, valor_atual, status) VALUES (%s, %s, %s, %s, %s, %s, %s, 0, 'ativa')").format(schema=sql.Identifier(user_schema))
            cur.execute(insert_query, (descricao, categoria, prazo_meses, valor_alvo, valor_mensal_sugerido, data_inicio, data_conclusao_prevista))
            flash('Nova meta definida!', 'success')
        conn.commit()

    except psycopg2.Error as e: conn.rollback(); flash(f'Erro DB meta: {e}', 'danger'); logging.error(f"Erro DB salvar meta {user_schema}: {e}")
    except Exception as e: conn.rollback(); flash(f'Erro inesperado meta: {e}', 'danger'); logging.error(f"Erro inesperado salvar meta {user_schema}: {e}")
    finally:
        if cur: cur.close()
        if conn: conn.close()

    return redirect(url_for('dashboard'))

@app.route('/cancelar_meta', methods=['POST'])
def cancelar_meta():
    if 'user_assinatura_id' not in session: flash('Sessão expirada.', 'warning'); return redirect(url_for('auth.login')) # Atualizado
    user_schema = session.get('user_schema'); redirect_url = url_for('dashboard')
    if not user_schema: flash('Erro interno.', 'danger'); return redirect(url_for('auth.login')) # Atualizado
    conn = get_db_connection();
    if not conn: flash('Erro conexão DB.', 'danger'); return redirect(redirect_url)
    cur = None
    try:
        cur = conn.cursor()
        update_query = sql.SQL("UPDATE {schema}.metas SET status = 'cancelada', atualizado_em = NOW() WHERE status = 'ativa'").format(schema=sql.Identifier(user_schema))
        cur.execute(update_query); conn.commit()
        # CORREÇÃO: Mover if para dentro do try
        if cur.rowcount > 0:
            flash('Meta cancelada.', 'success')
        else:
            flash('Nenhuma meta ativa encontrada.', 'info')
    except psycopg2.Error as e: conn.rollback(); flash(f'Erro DB: {e}', 'danger'); logging.error(f"Erro DB cancelar meta {user_schema}: {e}")
    except Exception as e: conn.rollback(); flash(f'Erro inesperado: {e}', 'danger'); logging.error(f"Erro inesperado cancelar meta {user_schema}: {e}")
    finally:
        if cur: cur.close()
        if conn: conn.close()
    return redirect(redirect_url)

@app.route('/add_progresso_meta', methods=['POST'])
def add_progresso_meta():
    if 'user_assinatura_id' not in session:
        flash('Sessão expirada.', 'warning')
        return redirect(url_for('auth.login')) # Atualizado

    user_schema = session.get('user_schema')
    redirect_url = url_for('dashboard') # Sempre redireciona para dashboard nesta ação
    if not user_schema:
        flash('Erro interno.', 'danger')
        return redirect(url_for('auth.login')) # Atualizado

    valor_adicionado_str = request.form.get('valor_progresso')

    if not valor_adicionado_str:
        flash('Erro: Informe o valor a ser adicionado ao progresso da meta.', 'danger')
        return redirect(redirect_url)

    try:
        valor_adicionado = Decimal(valor_adicionado_str.replace(',', '.'))
        if valor_adicionado <= 0:
            raise ValueError("O valor adicionado à meta deve ser positivo.")
    except (InvalidOperation, ValueError) as e:
        flash(f'Erro: Valor de progresso inválido. {e}', 'danger')
        return redirect(redirect_url)

    conn = get_db_connection()
    if not conn:
        flash('Erro de conexão com o banco.', 'danger')
        return redirect(redirect_url)

    cur = None
    try:
        cur = conn.cursor(cursor_factory=DictCursor)
        query_meta = sql.SQL("SELECT id, valor_alvo, valor_atual FROM {schema}.metas WHERE status = 'ativa' LIMIT 1").format(schema=sql.Identifier(user_schema))
        cur.execute(query_meta)
        meta = cur.fetchone()

        # CORREÇÃO APLICADA AQUI:
        if not meta:
            flash('Nenhuma meta ativa encontrada para adicionar progresso.', 'warning')
            # Não feche a conexão aqui, o finally cuidará disso
            return redirect(redirect_url)

        novo_valor_atual = meta['valor_atual'] + valor_adicionado
        status_meta_final = 'ativa'
        if novo_valor_atual >= meta['valor_alvo']:
            novo_valor_atual = meta['valor_alvo'] # Limita ao valor alvo
            status_meta_final = 'concluida'
            flash(f'Parabéns! Meta de {format_currency_filter(meta["valor_alvo"])} concluída!', 'success')
        else:
            flash(f'Progresso de {format_currency_filter(valor_adicionado)} adicionado à meta!', 'success')

        update_query = sql.SQL("""
            UPDATE {schema}.metas
            SET valor_atual = %s, status = %s, atualizado_em = NOW()
            WHERE id = %s AND status = 'ativa'
        """).format(schema=sql.Identifier(user_schema))
        cur.execute(update_query, (novo_valor_atual, status_meta_final, meta['id']))
        conn.commit()

        # Verificar se a atualização realmente ocorreu (caso a meta tenha sido alterada entre o SELECT e o UPDATE)
        if cur.rowcount == 0 and status_meta_final == 'ativa':
             flash('Não foi possível adicionar progresso. A meta pode ter sido alterada ou cancelada.', 'warning')

    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash(f'Erro ao adicionar progresso: {e}', 'danger')
        logging.error(f"Erro DB ao adicionar progresso meta para schema {user_schema}: {e}")
    except Exception as e:
        if conn: conn.rollback()
        flash(f'Erro inesperado ao adicionar progresso: {e}', 'danger')
        logging.error(f"Erro inesperado ao adicionar progresso meta para schema {user_schema}: {e}")
    finally:
        if cur: cur.close()
        if conn: conn.close()

    return redirect(redirect_url)


# --- Rota Relatórios (Com Correção de Sintaxe e Logs) ---
@app.route('/relatorios')
def relatorios():
    if 'user_assinatura_id' not in session:
        flash('Você precisa fazer login para acessar esta página.', 'warning')
        return redirect(url_for('auth.login')) # Atualizado

    user_schema = session.get('user_schema')
    user_nome = session.get('user_nome', session.get('user_email'))
    if not user_schema:
        flash('Erro interno: Informações do usuário incompletas.', 'danger')
        session.clear()
        return redirect(url_for('auth.login')) # Atualizado

    # --- Processamento de Filtros ---
    today = date.today()
    default_start_date = today.replace(day=1)
    default_end_date = today
    data_inicio_str = request.args.get('data_inicio', default_start_date.strftime('%Y-%m-%d'))
    data_fim_str = request.args.get('data_fim', default_end_date.strftime('%Y-%m-%d'))
    tipo_transacao_filtro = request.args.get('tipo_transacao', 'gastos_variaveis')
    categoria_filtro = request.args.get('categoria_filtro', 'todas')
    page = request.args.get('page', 1, type=int) # Obtém a página atual

    valid_tipos = ['receitas', 'gastos_variaveis', 'gastos_fixos']
    if tipo_transacao_filtro not in valid_tipos:
        tipo_transacao_filtro = 'gastos_variaveis'

    # Validação das datas
    try: data_inicio = datetime.strptime(data_inicio_str, '%Y-%m-%d').date()
    except ValueError: data_inicio = default_start_date; flash('Data de início inválida.', 'warning')
    try: data_fim = datetime.strptime(data_fim_str, '%Y-%m-%d').date()
    except ValueError: data_fim = default_end_date; flash('Data de fim inválida.', 'warning')
    if data_fim < data_inicio: data_fim = data_inicio; flash('Data fim anterior a início.', 'warning')

    filtros_aplicados = {
        'data_inicio': data_inicio.strftime('%Y-%m-%d'),
        'data_fim': data_fim.strftime('%Y-%m-%d'),
        'tipo_transacao': tipo_transacao_filtro,
        'categoria_filtro': categoria_filtro
        # 'page' não precisa ser passado aqui, é usado diretamente nos links
    }

    # --- Inicialização dos dados ---
    dados_relatorio = { "total_receitas": Decimal('0.00'), "total_despesas": Decimal('0.00'), "comparativo_percentual": None, "comparativo_valor_anterior": None }
    dados_grafico = { "labels": [], "datasets": { "receitas": [], "despesas": [] } }
    categorias_disponiveis = {'receitas': [], 'variaveis': [], 'fixas': []}
    # transacoes_raw = [] # Esta variável será preenchida de forma diferente dependendo do tipo
    lista_transacoes_paginada = [] # Lista final para o template (apenas itens da página)
    total_items = 0
    total_pages = 1
    current_page = page

    conn = get_db_connection()
    if not conn:
        flash('Erro de conexão com o banco.', 'danger')
        return render_template('relatorios.html', user_nome=user_nome, dados_relatorio=dados_relatorio,
                               dados_grafico=dados_grafico, categorias_disponiveis=categorias_disponiveis,
                               filtros_aplicados=filtros_aplicados, lista_transacoes=lista_transacoes_paginada,
                               current_page=1, total_pages=1) # Valores padrão de paginação

    cur = None
    try:
        cur = conn.cursor(cursor_factory=DictCursor)

        # --- Buscar Categorias Disponíveis (igual a antes) ---
        query_cat_rec = sql.SQL("SELECT DISTINCT categoria FROM {}.outras_receitas WHERE categoria IS NOT NULL ORDER BY categoria").format(sql.Identifier(user_schema)); cur.execute(query_cat_rec); categorias_disponiveis['receitas'] = [r['categoria'] for r in cur.fetchall()]
        query_cat_var = sql.SQL("SELECT DISTINCT categoria FROM {}.gastos WHERE categoria IS NOT NULL ORDER BY categoria").format(sql.Identifier(user_schema)); cur.execute(query_cat_var); categorias_disponiveis['variaveis'] = [r['categoria'] for r in cur.fetchall()]
        query_cat_fix = sql.SQL("SELECT DISTINCT categoria FROM {}.gastos_fixos WHERE categoria IS NOT NULL ORDER BY categoria").format(sql.Identifier(user_schema)); cur.execute(query_cat_fix); categorias_disponiveis['fixas'] = [r['categoria'] for r in cur.fetchall()]

        # --- Calcular Totais para dados_relatorio e dados_grafico (ANTES DA PAGINAÇÃO DE EXIBIÇÃO) ---
        # Esta parte calcula os totais e os dados para os gráficos usando todos os dados filtrados,
        # independentemente da página que será exibida na tabela.
        total_receitas_periodo_completo = Decimal('0.00')
        total_despesas_periodo_completo = Decimal('0.00')
        receitas_diarias_completo = {}
        despesas_diarias_completo = {}
        dias_no_periodo_list = []

        if data_inicio <= data_fim:
            query_dias = sql.SQL("SELECT generate_series(%s::date, %s::date, '1 day'::interval)::date as dia")
            cur.execute(query_dias, (data_inicio, data_fim))
            dias_no_periodo_list = [r['dia'] for r in cur.fetchall()]
            receitas_diarias_completo = {d: Decimal(0) for d in dias_no_periodo_list}
            despesas_diarias_completo = {d: Decimal(0) for d in dias_no_periodo_list}

        # Salário (para cálculo de total de receitas do período completo) - REMOVIDO
        # salario_config_completo = Decimal('0.00')
        # query_config_completo = sql.SQL("SELECT ingreso FROM {}.config WHERE id = 1").format(sql.Identifier(user_schema))
        # cur.execute(query_config_completo)
        # config_data_completo = cur.fetchone()
        # if config_data_completo and config_data_completo['ingreso'] is not None:
        #     salario_config_completo = config_data_completo['ingreso']

        # Calcular total de receitas do período completo (Outras Receitas)
        where_receitas_completo = [sql.SQL("fecha BETWEEN %s AND %s")]
        params_receitas_completo = [data_inicio, data_fim]
        # Não aplicamos filtro de categoria aqui, pois queremos o total para o gráfico e resumo
        query_sum_receitas_completo = sql.SQL(
            "SELECT fecha, valor FROM {schema}.outras_receitas WHERE {where}"
        ).format(schema=sql.Identifier(user_schema), where=sql.SQL(' AND ').join(where_receitas_completo))
        cur.execute(query_sum_receitas_completo, params_receitas_completo)
        for rec in cur.fetchall():
            total_receitas_periodo_completo += rec['valor']
            if rec['fecha'] in receitas_diarias_completo:
                receitas_diarias_completo[rec['fecha']] += rec['valor']
        
        # Adicionar salário ao total de receitas do período completo - REMOVIDO
        # if salario_config_completo > 0:
        #     for dia1_completo in list(rrule(MONTHLY, dtstart=data_inicio, until=data_fim, bymonthday=1)):
        #         dia_date_completo = dia1_completo.date()
        #         if data_inicio <= dia_date_completo <= data_fim:
        #             total_receitas_periodo_completo += salario_config_completo
        #             if dia_date_completo in receitas_diarias_completo:
        #                 receitas_diarias_completo[dia_date_completo] += salario_config_completo

        # Calcular total de despesas do período completo (Gastos Variáveis)
        where_gastos_var_completo = [sql.SQL("data BETWEEN %s AND %s")]
        params_gastos_var_completo = [data_inicio, data_fim]
        query_sum_gastos_var_completo = sql.SQL(
            "SELECT data, valor FROM {schema}.gastos WHERE {where}"
        ).format(schema=sql.Identifier(user_schema), where=sql.SQL(' AND ').join(where_gastos_var_completo))
        cur.execute(query_sum_gastos_var_completo, params_gastos_var_completo)
        for gv_comp in cur.fetchall():
            total_despesas_periodo_completo += gv_comp['valor']
            if gv_comp['data'] in despesas_diarias_completo:
                despesas_diarias_completo[gv_comp['data']] += gv_comp['valor']

        # Calcular total de despesas do período completo (Gastos Fixos)
        where_gf_completo = [sql.SQL("activo = TRUE"), sql.SQL("fecha_inicio <= %s")]
        params_gf_completo = [data_fim]
        query_base_fixos_completo = sql.SQL(
            "SELECT fecha_inicio, valor, recurrencia FROM {schema}.gastos_fixos WHERE {where}"
        ).format(schema=sql.Identifier(user_schema), where=sql.SQL(' AND ').join(where_gf_completo))
        cur.execute(query_base_fixos_completo, params_gf_completo)
        gastos_fixos_ativos_completo = cur.fetchall()
        for gf_comp in gastos_fixos_ativos_completo:
            rrule_params_comp = get_rrule_params(gf_comp['recurrencia'])
            if rrule_params_comp:
                try:
                    occurrences_comp = list(rrule(dtstart=gf_comp['fecha_inicio'], until=data_fim, **rrule_params_comp))
                    for occ_dt_comp in occurrences_comp:
                        occ_date_comp = occ_dt_comp.date()
                        if data_inicio <= occ_date_comp <= data_fim:
                            total_despesas_periodo_completo += gf_comp['valor']
                            if occ_date_comp in despesas_diarias_completo:
                                despesas_diarias_completo[occ_date_comp] += gf_comp['valor']
                except Exception as e_rrule_comp:
                    logging.error(f"Relatorios (completo) Gasto Fixo rrule error: {e_rrule_comp}")
            elif gf_comp['recurrencia'].lower().strip() in ['unico', 'único', 'única'] and data_inicio <= gf_comp['fecha_inicio'] <= data_fim:
                total_despesas_periodo_completo += gf_comp['valor']
                if gf_comp['fecha_inicio'] in despesas_diarias_completo:
                    despesas_diarias_completo[gf_comp['fecha_inicio']] += gf_comp['valor']
        
        dados_relatorio['total_receitas'] = total_receitas_periodo_completo
        dados_relatorio['total_despesas'] = total_despesas_periodo_completo

        dados_grafico['labels'] = [d.strftime('%d/%m') for d in dias_no_periodo_list]
        dados_grafico['datasets']['receitas'] = [float(receitas_diarias_completo.get(dia, 0)) for dia in dias_no_periodo_list]
        dados_grafico['datasets']['despesas'] = [float(despesas_diarias_completo.get(dia, 0)) for dia in dias_no_periodo_list]
        
        # --- FIM DO CÁLCULO DE TOTAIS PARA GRÁFICOS E RESUMO ---

        # --- Início da Lógica de Busca Paginada para a Tabela ---
        query_params_paginado = []
        where_clauses_paginado = []
        
        # Salário (para exibição na tabela de receitas, se aplicável) - REMOVIDO
        # salario_config_tabela = Decimal('0.00')
        # Reutiliza a query de config já feita, se não for problema de performance.
        # Caso contrário, buscaria de novo: query_config_tab = sql.SQL("SELECT ingreso FROM {}.config WHERE id = 1")...
        # if config_data_completo and config_data_completo['ingreso'] is not None: # Reutilizando config_data_completo
        #     salario_config_tabela = config_data_completo['ingreso']


        if tipo_transacao_filtro == 'receitas':
            table_name = sql.Identifier('outras_receitas')
            date_column = sql.Identifier('fecha')
            select_cols = sql.SQL("id, fecha as data, descripcion, categoria, valor, 'receita' as tipo")
            
            where_clauses_paginado.append(sql.SQL("{date_col} BETWEEN %s AND %s").format(date_col=date_column))
            query_params_paginado.extend([data_inicio, data_fim])

            if categoria_filtro != 'todas':
                if categoria_filtro in categorias_disponiveis['receitas']:
                    where_clauses_paginado.append(sql.SQL("categoria = %s"))
                    query_params_paginado.append(categoria_filtro)
                else: # Categoria inválida, não busca nada
                    where_clauses_paginado.append(sql.SQL("1 = 0")) 
            
            # Para receitas, precisamos considerar o salário que não está na tabela `outras_receitas`
            # A paginação SQL direta é complexa com UNION ou dados fora da tabela.
            # Vamos buscar todas as "outras_receitas" filtradas e adicionar o salário ANTES de paginar em Python.
            # Esta é uma exceção à paginação SQL direta para 'receitas' devido ao salário.
            
            transacoes_raw_receitas = []
            if where_clauses_paginado: # Evita query se for "1=0"
                query_det_receitas_paginado = sql.SQL("SELECT {cols} FROM {schema}.{table} WHERE {where} ORDER BY {date_col} DESC, id DESC").format(
                    cols=select_cols, schema=sql.Identifier(user_schema), table=table_name, 
                    where=sql.SQL(' AND ').join(where_clauses_paginado), date_col=date_column
                )
                cur.execute(query_det_receitas_paginado, query_params_paginado)
                transacoes_raw_receitas.extend(cur.fetchall())

            # Adiciona salário se aplicável (APENAS se categoria_filtro for 'todas' ou 'Salário') - REMOVIDO
            # if salario_config_tabela > 0 and (categoria_filtro == 'todas' or categoria_filtro == 'Salário'):
            #     for dia1_salario in list(rrule(MONTHLY, dtstart=data_inicio, until=data_fim, bymonthday=1)):
            #         dia_date_salario = dia1_salario.date()
            #         if data_inicio <= dia_date_salario <= data_fim:
            #             transacoes_raw_receitas.append({
            #                 'id': None, 'data': dia_date_salario, 'descripcion': 'Salário Principal', 
            #                 'categoria': 'Salário', 'valor': salario_config_tabela, 'tipo': 'receita'
            #             })
            
            transacoes_raw_receitas.sort(key=lambda x: x['data'], reverse=True) # Ordena antes de paginar
            total_items = len(transacoes_raw_receitas)
            total_pages = ceil(total_items / ITEMS_PER_PAGE) if total_items > 0 else 1
            current_page = min(page, total_pages) if total_pages > 0 else 1
            offset = (current_page - 1) * ITEMS_PER_PAGE
            lista_transacoes_paginada = transacoes_raw_receitas[offset : offset + ITEMS_PER_PAGE]

        elif tipo_transacao_filtro == 'gastos_variaveis':
            table_name = sql.Identifier('gastos')
            date_column = sql.Identifier('data')
            select_cols = sql.SQL("id, data, descripcion, categoria, valor, 'gasto_variavel' as tipo")

            where_clauses_paginado.append(sql.SQL("{date_col} BETWEEN %s AND %s").format(date_col=date_column))
            query_params_paginado.extend([data_inicio, data_fim])

            if categoria_filtro != 'todas':
                if categoria_filtro in categorias_disponiveis['variaveis']:
                    where_clauses_paginado.append(sql.SQL("categoria = %s"))
                    query_params_paginado.append(categoria_filtro)
                else:
                    where_clauses_paginado.append(sql.SQL("1 = 0"))

            where_sql_paginado = sql.SQL(' AND ').join(where_clauses_paginado) if where_clauses_paginado else sql.SQL("1=1")

            # Count query
            count_query_gv = sql.SQL("SELECT COUNT(*) FROM {schema}.{table} WHERE {where}").format(
                schema=sql.Identifier(user_schema), table=table_name, where=where_sql_paginado
            )
            cur.execute(count_query_gv, query_params_paginado)
            total_items = cur.fetchone()[0]
            total_pages = ceil(total_items / ITEMS_PER_PAGE) if total_items > 0 else 1
            current_page = min(page, total_pages) if total_pages > 0 else 1
            offset = (current_page - 1) * ITEMS_PER_PAGE

            # Main query com LIMIT e OFFSET
            query_det_gastos_var_paginado = sql.SQL(
                "SELECT {cols} FROM {schema}.{table} WHERE {where} ORDER BY {date_col} DESC, id DESC LIMIT %s OFFSET %s"
            ).format(cols=select_cols, schema=sql.Identifier(user_schema), table=table_name, where=where_sql_paginado, date_col=date_column)
            
            params_main_gv = query_params_paginado + [ITEMS_PER_PAGE, offset]
            cur.execute(query_det_gastos_var_paginado, params_main_gv)
            lista_transacoes_paginada = cur.fetchall()

        elif tipo_transacao_filtro == 'gastos_fixos':
            # Mantém a lógica de expansão em Python e paginação em Python para gastos fixos
            transacoes_raw_fixos = []
            where_gf_pag = [sql.SQL("activo = TRUE"), sql.SQL("fecha_inicio <= %s")]
            params_gf_pag = [data_fim]
            if categoria_filtro != 'todas':
                if categoria_filtro in categorias_disponiveis['fixas']:
                    where_gf_pag.append(sql.SQL("categoria = %s"))
                    params_gf_pag.append(categoria_filtro)
                else:
                    where_gf_pag.append(sql.SQL("1 = 0")) # Categoria inválida, não busca nada
            
            query_base_fixos_pag = sql.SQL(
                "SELECT id, fecha_inicio, descripcion, categoria, valor, recurrencia FROM {schema}.gastos_fixos WHERE {where}"
            ).format(schema=sql.Identifier(user_schema), where=sql.SQL(' AND ').join(where_gf_pag))
            
            cur.execute(query_base_fixos_pag, params_gf_pag)
            gastos_fixos_ativos_pag = cur.fetchall()

            for gf_pag in gastos_fixos_ativos_pag:
                rrule_params_pag = get_rrule_params(gf_pag['recurrencia'])
                if rrule_params_pag:
                    try:
                        occurrences_pag = list(rrule(dtstart=gf_pag['fecha_inicio'], until=data_fim, **rrule_params_pag))
                        for occ_dt_pag in occurrences_pag:
                            occ_date_pag = occ_dt_pag.date()
                            if data_inicio <= occ_date_pag <= data_fim:
                                transacoes_raw_fixos.append({
                                    'data': occ_date_pag, 'descripcion': gf_pag['descripcion'], 
                                    'categoria': gf_pag['categoria'], 'valor': gf_pag['valor'], 
                                    'tipo': 'gasto_fixo', 'id': gf_pag['id']
                                })
                    except Exception as e_rrule_pag:
                        logging.error(f"Relatorios (paginado) Gasto Fixo ID {gf_pag['id']} rrule error: {e_rrule_pag}")
                elif gf_pag['recurrencia'].lower().strip() in ['unico', 'único', 'única'] and data_inicio <= gf_pag['fecha_inicio'] <= data_fim:
                    transacoes_raw_fixos.append({
                        'data': gf_pag['fecha_inicio'], 'descripcion': gf_pag['descripcion'], 
                        'categoria': gf_pag['categoria'], 'valor': gf_pag['valor'], 
                        'tipo': 'gasto_fixo', 'id': gf_pag['id']
                    })
            
            transacoes_raw_fixos.sort(key=lambda x: x['data'], reverse=True) # Ordena antes de paginar
            total_items = len(transacoes_raw_fixos)
            total_pages = ceil(total_items / ITEMS_PER_PAGE) if total_items > 0 else 1
            current_page = min(page, total_pages) if total_pages > 0 else 1
            offset = (current_page - 1) * ITEMS_PER_PAGE
            lista_transacoes_paginada = transacoes_raw_fixos[offset : offset + ITEMS_PER_PAGE]

        logging.info(f"Paginação Relatórios ({tipo_transacao_filtro}): Itens totais={total_items}, Páginas={total_pages}, Página Atual={current_page}, Offset={offset}, Itens na Página={len(lista_transacoes_paginada)}")
        # --- Fim da Lógica de Busca Paginada ---

        # Atribuições para dados_relatorio e dados_grafico já foram feitas acima com os totais completos.
        
        # --- Calcular Comparativo com Mês Anterior (igual a antes) ---
        # O cálculo do comparativo deve usar os totais do período ATUAL COMPLETO
        # (total_receitas_periodo_completo ou total_despesas_periodo_completo)
        # e recalcular os totais do período ANTERIOR da mesma forma (completo).
        dados_relatorio['comparativo_percentual'] = None
        dados_relatorio['comparativo_valor_anterior'] = None
        try:
            delta_dias = (data_fim - data_inicio).days
            data_inicio_anterior = data_inicio - relativedelta(months=1)
            # data_fim_anterior = data_inicio_anterior + timedelta(days=delta_dias) # Esta linha pode ser problemática se o mês anterior for mais curto
            
            # Ajusta data_fim_anterior para ser o mesmo dia do mês ou o último dia do mês anterior, o que vier primeiro
            try:
                data_fim_anterior = data_inicio_anterior.replace(day=data_fim.day)
            except ValueError: # Dia não existe no mês anterior (ex: dia 31 em fevereiro)
                data_fim_anterior = (data_inicio_anterior + relativedelta(months=1)).replace(day=1) - timedelta(days=1)

            # Garante que data_fim_anterior não ultrapasse o final do mês anterior nem seja antes do início
            ultimo_dia_mes_anterior_calc = (data_inicio_anterior + relativedelta(months=1)).replace(day=1) - timedelta(days=1)
            data_fim_anterior = min(data_fim_anterior, ultimo_dia_mes_anterior_calc)
            data_fim_anterior = max(data_fim_anterior, data_inicio_anterior)


            total_anterior_calculado = Decimal('0.00')
            total_atual_comparativo = Decimal('0.00')

            # Recalcular o total do período anterior para o tipo de transação selecionado
            if tipo_transacao_filtro == 'receitas':
                total_atual_comparativo = total_receitas_periodo_completo # Usa o total completo já calculado
                # Calcular receitas do período anterior
                where_rec_ant = [sql.SQL("fecha BETWEEN %s AND %s")]
                params_rec_ant = [data_inicio_anterior, data_fim_anterior]
                if categoria_filtro != 'todas' and categoria_filtro != 'Salário': # Salário é tratado separadamente
                    where_rec_ant.append(sql.SQL("categoria = %s"))
                    params_rec_ant.append(categoria_filtro)
                elif categoria_filtro == 'Salário': # Se o filtro é apenas Salário
                     where_rec_ant.append(sql.SQL("1 = 0")) # Não busca outras receitas

                if not (categoria_filtro == 'Salário' and not where_rec_ant[0] == sql.SQL("1 = 0")): # Evita query desnecessária se for só salário
                    query_sum_rec_ant = sql.SQL("SELECT SUM(valor) as total FROM {schema}.outras_receitas WHERE {where}").format(
                        schema=sql.Identifier(user_schema), where=sql.SQL(' AND ').join(where_rec_ant))
                    cur.execute(query_sum_rec_ant, params_rec_ant)
                    res_rec_ant = cur.fetchone()
                    if res_rec_ant and res_rec_ant['total']:
                        total_anterior_calculado += res_rec_ant['total']
                
                # Adição de salário ao período anterior - REMOVIDO
                # if salario_config_tabela > 0 and (categoria_filtro == 'todas' or categoria_filtro == 'Salário'):
                #     for dia1_sal_ant in list(rrule(MONTHLY, dtstart=data_inicio_anterior, until=data_fim_anterior, bymonthday=1)):
                #         if data_inicio_anterior <= dia1_sal_ant.date() <= data_fim_anterior:
                #             total_anterior_calculado += salario_config_tabela
            
            elif tipo_transacao_filtro == 'gastos_variaveis':
                total_atual_comparativo = total_despesas_periodo_completo # Usa o total completo já calculado
                where_gv_ant = [sql.SQL("data BETWEEN %s AND %s")]
                params_gv_ant = [data_inicio_anterior, data_fim_anterior]
                if categoria_filtro != 'todas':
                    where_gv_ant.append(sql.SQL("categoria = %s"))
                    params_gv_ant.append(categoria_filtro)
                query_sum_gv_ant = sql.SQL("SELECT SUM(valor) as total FROM {schema}.gastos WHERE {where}").format(
                    schema=sql.Identifier(user_schema), where=sql.SQL(' AND ').join(where_gv_ant))
                cur.execute(query_sum_gv_ant, params_gv_ant)
                res_gv_ant = cur.fetchone()
                if res_gv_ant and res_gv_ant['total']:
                    total_anterior_calculado += res_gv_ant['total']

            elif tipo_transacao_filtro == 'gastos_fixos':
                total_atual_comparativo = total_despesas_periodo_completo # Usa o total completo já calculado
                where_gf_ant_base = [sql.SQL("activo = TRUE"), sql.SQL("fecha_inicio <= %s")]
                params_gf_ant_base = [data_fim_anterior]
                if categoria_filtro != 'todas':
                    where_gf_ant_base.append(sql.SQL("categoria = %s"))
                    params_gf_ant_base.append(categoria_filtro)
                
                query_gf_ant = sql.SQL("SELECT fecha_inicio, valor, recurrencia FROM {schema}.gastos_fixos WHERE {where}").format(
                    schema=sql.Identifier(user_schema), where=sql.SQL(' AND ').join(where_gf_ant_base))
                cur.execute(query_gf_ant, params_gf_ant_base)
                gastos_fixos_ant = cur.fetchall()
                for gf_ant in gastos_fixos_ant:
                    rrule_params_ant = get_rrule_params(gf_ant['recurrencia'])
                    if rrule_params_ant:
                        occurrences_ant = list(rrule(dtstart=gf_ant['fecha_inicio'], until=data_fim_anterior, **rrule_params_ant))
                        for occ_dt_ant in occurrences_ant:
                            if data_inicio_anterior <= occ_dt_ant.date() <= data_fim_anterior:
                                total_anterior_calculado += gf_ant['valor']
                    elif gf_ant['recurrencia'].lower().strip() in ['unico', 'único', 'única'] and data_inicio_anterior <= gf_ant['fecha_inicio'] <= data_fim_anterior:
                        total_anterior_calculado += gf_ant['valor']
            
            if total_anterior_calculado > 0: # Evita divisão por zero
                dados_relatorio['comparativo_valor_anterior'] = total_anterior_calculado
                variacao = total_atual_comparativo - total_anterior_calculado
                percentual = (variacao / total_anterior_calculado) * 100
                dados_relatorio['comparativo_percentual'] = percentual.quantize(Decimal('0.1'), rounding=ROUND_HALF_UP)
            else: # Se o total anterior for zero
                dados_relatorio['comparativo_valor_anterior'] = total_anterior_calculado # Será 0
                dados_relatorio['comparativo_percentual'] = None # Ou algum indicador de que não há base para comparação

        except Exception as e_comp:
            logging.warning(f"Erro ao calcular comparativo: {e_comp}")
            # Mantém os valores como None em caso de erro
        
        logging.info(f"Dados de relatório calculados para {user_schema}. Período: {data_inicio_str} a {data_fim_str}")

    except psycopg2.Error as e:
        logging.error(f"Erro DB /relatorios {user_schema}: {e}")
        dados_grafico['datasets']['receitas'] = [float(receitas_diarias.get(dia, 0)) for dia in dias_no_periodo_list]
        dados_grafico['datasets']['despesas'] = [float(despesas_diarias.get(dia, 0)) for dia in dias_no_periodo_list]

        # --- Calcular Comparativo com Mês Anterior (igual a antes) ---
        # (código do comparativo omitido para brevidade, mas permanece o mesmo)
        dados_relatorio['comparativo_percentual'] = None; dados_relatorio['comparativo_valor_anterior'] = None
        try:
            delta_dias = (data_fim - data_inicio).days; data_inicio_anterior = data_inicio - relativedelta(months=1)
            data_fim_anterior = data_inicio_anterior + timedelta(days=delta_dias)
            ultimo_dia_mes_anterior = (data_inicio_anterior + relativedelta(months=1)).replace(day=1) - timedelta(days=1)
            if data_fim_anterior > ultimo_dia_mes_anterior: data_fim_anterior = ultimo_dia_mes_anterior
            if data_fim_anterior < data_inicio_anterior: data_fim_anterior = data_inicio_anterior
            total_anterior = Decimal('0.00'); total_atual = Decimal('0.00')
            # Lógica de cálculo do total_anterior para cada tipo (receitas, gastos_v, gastos_f)
            # ... (código exato do comparativo aqui) ...
            if tipo_transacao_filtro == 'receitas':
                # ... cálculo total_anterior para receitas ...
                total_atual = dados_relatorio['total_receitas']
            elif tipo_transacao_filtro == 'gastos_variaveis':
                 # ... cálculo total_anterior para gastos variáveis ...
                total_atual = dados_relatorio['total_despesas']
            elif tipo_transacao_filtro == 'gastos_fixos':
                 # ... cálculo total_anterior para gastos fixos ...
                total_atual = dados_relatorio['total_despesas']

            if total_anterior > 0:
                dados_relatorio['comparativo_valor_anterior'] = total_anterior
                variacao = total_atual - total_anterior
                percentual = (variacao / total_anterior) * 100
                dados_relatorio['comparativo_percentual'] = percentual.quantize(Decimal('0.1'), rounding=ROUND_HALF_UP)
            else:
                dados_relatorio['comparativo_valor_anterior'] = total_anterior
                dados_relatorio['comparativo_percentual'] = None
        except Exception as e_comp:
            logging.warning(f"Erro ao calcular comparativo: {e_comp}")
            dados_relatorio['comparativo_percentual'] = None; dados_relatorio['comparativo_valor_anterior'] = None


        logging.info(f"Dados de relatório calculados para {user_schema}. Período: {data_inicio_str} a {data_fim_str}")

    except psycopg2.Error as e:
        logging.error(f"Erro DB /relatorios {user_schema}: {e}")
        flash('Erro ao buscar dados para o relatório.', 'danger')
        # Resetar dados para evitar erro no template
        lista_transacoes_paginada = []; total_pages = 1; current_page = 1
    except Exception as e:
        logging.error(f"Erro inesperado /relatorios {user_schema}: {e}", exc_info=True)
        flash('Erro inesperado ao gerar relatório.', 'danger')
        lista_transacoes_paginada = []; total_pages = 1; current_page = 1
    finally:
        if cur: cur.close()
        if conn: conn.close()

    # Renderiza o template passando a lista PAGINADA e as informações de paginação
    return render_template('relatorios.html',
                           user_nome=user_nome,
                           dados_relatorio=dados_relatorio,
                           dados_grafico=dados_grafico,
                           categorias_disponiveis=categorias_disponiveis,
                           filtros_aplicados=filtros_aplicados,
                           lista_transacoes=lista_transacoes_paginada, # Lista paginada
                           current_page=current_page, # Página atual
                           total_pages=total_pages)   # Total de páginas


# Importa o Blueprint de autenticação
from auth import auth_bp
app.register_blueprint(auth_bp)

# ... (resto do app.py, incluindo if __name__ == '__main__':) ...
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    # Certifique-se de que debug=False em produção
    # A importação de get_db_connection em auth.py agora deve funcionar
    app.run(host='0.0.0.0', port=port, debug=True)
