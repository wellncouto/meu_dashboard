import os
import re
import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor
from flask import Blueprint, render_template, request, redirect, url_for, session, flash
from werkzeug.security import generate_password_hash, check_password_hash
import logging

# Importação da conexão com o banco de dados de app.py
from app import get_db_connection 

# Blueprint Configuration
auth_bp = Blueprint(
    'auth', __name__,
    template_folder='templates', 
    static_folder='static' 
)

# --- Funções Auxiliares de Autenticação ---
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

# --- Rotas de Autenticação ---
@auth_bp.route('/') # Adicionado para redirecionar a raiz do blueprint, se necessário
def index_redirect():
    # Redireciona para a rota de login dentro do blueprint de autenticação
    return redirect(url_for('auth.login'))


@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    # A importação de get_db_connection agora é feita no nível do módulo.

    if 'user_assinatura_id' in session:
        return redirect(url_for('dashboard')) # 'dashboard' ainda está em app.py
    if request.method == 'POST':
        email = request.form.get('email')
        senha = request.form.get('senha')
        if not email or not senha:
            flash('Email e senha são obrigatórios.', 'danger')
            return redirect(url_for('auth.login'))
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
                            logging.info(f"Login bem-sucedido: {login_user['email']}, Schema: {schema_name}")
                            return redirect(url_for('dashboard')) # 'dashboard' ainda está em app.py
                        else:
                            logging.error(f"Não foi possível gerar nome do schema para usuário {email}.")
                            flash('Erro interno ao determinar o schema do usuário.', 'danger')
                    else:
                        logging.error(f"Assinatura ID {login_user['id_cliente_assinatura']} não encontrada para usuário {email}.")
                        flash('Erro interno: dados da assinatura não encontrados.', 'danger')
                else:
                    logging.warning(f"Tentativa de login falhou para: {email} (email não cadastrado ou senha incorreta)")
                    flash('Email ou senha incorretos.', 'danger')
            except psycopg2.Error as e:
                logging.error(f"Erro de banco de dados durante o login para {email}: {e}")
                flash('Erro no banco de dados durante o login.', 'danger')
            finally:
                if cur: cur.close()
                if conn: conn.close()
        else:
            flash('Não foi possível conectar ao banco de dados.', 'danger')
        return render_template('login.html')
    return render_template('login.html')


@auth_bp.route('/criar-conta', methods=['GET', 'POST'])
def criar_conta():
    # A importação de get_db_connection agora é feita no nível do módulo.
    if request.method == 'POST':
        email = request.form.get('email')
        senha = request.form.get('senha')
        senha_confirmacao = request.form.get('senha_confirmacao')

        if not email or not senha or not senha_confirmacao:
            flash('Preencha email, senha e confirmação.', 'danger')
            return render_template('criar_conta.html', email_previo=email)

        if senha != senha_confirmacao:
            flash('As senhas digitadas não conferem.', 'danger')
            return render_template('criar_conta.html', email_previo=email)

        conn = get_db_connection()
        if not conn:
            flash('Erro crítico: Não foi possível conectar ao banco de dados.', 'danger')
            return redirect(url_for('auth.login'))

        cur = None
        try:
            cur = conn.cursor(cursor_factory=DictCursor)
            cur.execute("SELECT id FROM clientes.dashboard_usuarios WHERE email = %s", (email,))
            if cur.fetchone():
                flash('Este email já possui um acesso ao dashboard. Tente fazer login ou recuperar a senha.', 'warning')
                return redirect(url_for('auth.login'))

            cur.execute("SELECT id_interno FROM clientes.assinaturas WHERE email = %s LIMIT 1", (email,))
            assinatura = cur.fetchone()

            if not assinatura:
                flash('Email não encontrado em nossas assinaturas ativas. Verifique se digitou corretamente ou entre em contato.', 'danger')
                return render_template('criar_conta.html', email_previo=email)

            id_cliente_assinatura_encontrado = assinatura['id_interno']
            senha_hashed = gerar_hash_senha(senha)

            insert_query = sql.SQL("""
                INSERT INTO clientes.dashboard_usuarios (email, senha_hash, id_cliente_assinatura)
                VALUES (%s, %s, %s)
            """)
            cur.execute(insert_query, (email, senha_hashed, id_cliente_assinatura_encontrado))
            conn.commit()

            flash('¡Acceso al panel creado con éxito! Ya puedes iniciar sesión.', 'success')
            logging.info(f"Novo acesso dashboard criado para email: {email}, ID Assinatura: {id_cliente_assinatura_encontrado}")
            return redirect(url_for('auth.login'))

        except psycopg2.Error as e:
            conn.rollback()
            flash('Error en la base de datos al intentar crear el acceso. Intenta nuevamente o contacta al soporte.', 'danger')
            logging.error(f"Erro DB (criar_conta) para {email}: {e}")
            return render_template('criar_conta.html', email_previo=email)
        except Exception as e:
            conn.rollback()
            flash('Ocorreu um erro inesperado. Tente novamente mais tarde.', 'danger')
            logging.error(f"Erro inesperado (criar_conta) para {email}: {e}", exc_info=True)
            return render_template('criar_conta.html', email_previo=email)
        finally:
            if cur: cur.close()
            if conn: conn.close()
    else: # Método GET
        return render_template('criar_conta.html')

@auth_bp.route('/logout')
def logout():
    session.clear()
    flash('Cerraste sesión de tu cuenta.', 'info')
    return redirect(url_for('auth.login'))

@auth_bp.route('/esqueci-senha', methods=['GET'])
def esqueci_senha_request():
    # TODO: Implementar lógica de solicitar reset e template
    # return render_template('esqueci_senha_request.html') # Quando criar o template
    return "Página para Solicitar Redefinição de Senha (Em construção) - Blueprint"
