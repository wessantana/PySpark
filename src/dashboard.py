import streamlit as st
import pandas as pd
import os
import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import psycopg2
from psycopg2 import sql
import hashlib
from datetime import datetime, timedelta
import random
import time

load_dotenv(dotenv_path="/app/.env")
print(os.getenv("SMTP_SERVER"), os.getenv("SMTP_USER"), os.getenv("SMTP_PASS"))

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            sslmode="require"
        )
        return conn
    except psycopg2.Error as e:
        st.error(f"Erro ao conectar ao banco de dados: {e}")
        return None

def init_db():
    conn = get_db_connection()
    if conn is None:
        return
        
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                email VARCHAR(255) UNIQUE NOT NULL,
                password VARCHAR(255) NOT NULL,
                name VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mfa_codes (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                code VARCHAR(6) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP NOT NULL,
                used BOOLEAN DEFAULT FALSE
            )
        """)
        
        conn.commit()
    except psycopg2.Error as e:
        st.error(f"Erro ao inicializar banco de dados: {e}")
    finally:
        if conn:
            conn.close()

init_db()

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def create_user(email, password, name):
    conn = get_db_connection()
    if conn is None:
        return False
        
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO users (email, password, name) VALUES (%s, %s, %s)",
            (email, hash_password(password), name))
        conn.commit()
        return True
    except psycopg2.IntegrityError:
        return False
    finally:
        if conn:
            conn.close()

def verify_user(email, password):
    conn = get_db_connection()
    if conn is None:
        return None
        
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, email, name FROM users WHERE email = %s AND password = %s",
            (email, hash_password(password)))
        user = cur.fetchone()
        return {
            "id": user[0],
            "email": user[1],
            "name": user[2]
        } if user else None
    except psycopg2.Error as e:
        st.error(f"Erro ao verificar usuário: {e}")
        return None
    finally:
        if conn:
            conn.close()

def generate_mfa_code(user_id):
    code = str(random.randint(100000, 999999))
    expires_at = datetime.now() + timedelta(minutes=10)
    
    conn = get_db_connection()
    if conn is None:
        return None
        
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE mfa_codes SET used = TRUE WHERE user_id = %s AND used = FALSE",
            (user_id,))
        
        cur.execute(
            "INSERT INTO mfa_codes (user_id, code, expires_at) VALUES (%s, %s, %s)",
            (user_id, code, expires_at))
        conn.commit()
        return code
    except psycopg2.Error as e:
        st.error(f"Erro ao gerar código MFA: {e}")
        return None
    finally:
        if conn:
            conn.close()

def verify_mfa_code(user_id, code):
    conn = get_db_connection()
    if conn is None:
        return False
        
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id FROM mfa_codes WHERE user_id = %s AND code = %s AND used = FALSE AND expires_at > NOW()",
            (user_id, code))
        result = cur.fetchone()
        
        if result:
            cur.execute(
                "UPDATE mfa_codes SET used = TRUE WHERE id = %s",
                (result[0],))
            conn.commit()
            return True
        return False
    except psycopg2.Error as e:
        st.error(f"Erro ao verificar código MFA: {e}")
        return False
    finally:
        if conn:
            conn.close()

def send_email(to_email, subject, body):
    smtp_server = os.getenv("SMTP_SERVER")
    smtp_port = 465
    smtp_user = os.getenv("SMTP_USER")
    smtp_pass = os.getenv("SMTP_PASS")

    if not all([smtp_server, smtp_port, smtp_user, smtp_pass]):
        st.error("Configurações de email não encontradas no arquivo .env")
        return False

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = to_email

    try:
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, to_email, msg.as_string())
        return True
    except Exception as e:
        st.error(f"Erro ao enviar email: {e}")
        return False

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
    st.session_state.mfa_verified = False
    st.session_state.user = None
    st.session_state.mfa_code = None
    st.session_state.mfa_sent_time = None

if not st.session_state.authenticated or not st.session_state.mfa_verified:
    auth_tab, register_tab = st.tabs(["Login", "Cadastro"])
    
    with auth_tab:
        st.header("Login")
        email = st.text_input("Email", key="login_email")
        password = st.text_input("Senha", type="password", key="login_password")
        
        if st.button("Entrar"):
            user = verify_user(email, password)
            if user:
                st.session_state.user = user
                mfa_code = generate_mfa_code(user["id"])
                if mfa_code:
                    if send_email(
                        user["email"],
                        "Seu código de verificação",
                        f"Seu código de verificação é: {mfa_code}\n\nO código é válido por 10 minutos."
                    ):
                        st.session_state.mfa_sent_time = time.time()
                        st.success("Código de verificação enviado para seu email!")
                    else:
                        st.error("Falha ao enviar código de verificação")
                else:
                    st.error("Falha ao gerar código de verificação")
            else:
                st.error("Email ou senha incorretos")
        
        if st.session_state.user and not st.session_state.mfa_verified:
            st.divider()
            st.subheader("Verificação em Duas Etapas")
            mfa_code = st.text_input("Digite o código de 6 dígitos enviado para seu email")
            
            if st.button("Verificar Código"):
                if verify_mfa_code(st.session_state.user["id"], mfa_code):
                    st.session_state.mfa_verified = True
                    st.session_state.authenticated = True
                    st.success("Verificação bem-sucedida!")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("Código inválido ou expirado")
            
            if st.session_state.mfa_sent_time:
                elapsed = time.time() - st.session_state.mfa_sent_time
                remaining = max(0, 600 - elapsed)  # 10 minutos = 600 segundos
                st.write(f"Tempo restante: {int(remaining // 60)}:{int(remaining % 60):02d}")
    
    with register_tab:
        st.header("Cadastro")
        name = st.text_input("Nome completo")
        email = st.text_input("Email", key="register_email")
        password = st.text_input("Senha", type="password", key="register_password")
        confirm_password = st.text_input("Confirmar senha", type="password")
        
        if st.button("Cadastrar"):
            if password != confirm_password:
                st.error("As senhas não coincidem")
            elif len(password) < 6:
                st.error("A senha deve ter pelo menos 6 caracteres")
            else:
                if create_user(email, password, name):
                    st.success("Cadastro realizado com sucesso! Faça login para continuar.")
                else:
                    st.error("Este email já está cadastrado")
    
    st.stop()

st.set_page_config(page_title="Dashboard de Acidentes", layout="wide")
st.title(f"Dashboard Interativo de Acidentes Rodoviários - Bem-vindo, {st.session_state.user['name']}")

processed_dir = "data/processed"
folders = [f for f in os.listdir(processed_dir) if f.startswith("processed")]

@st.cache_data
def load_data():
    dfs = []
    for folder in folders:
        parquet_path = os.path.join(processed_dir, folder)
        files = [os.path.join(parquet_path, f) for f in os.listdir(parquet_path) if f.endswith(".parquet")]
        for file in files:
            dfs.append(pd.read_parquet(file))
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame()

df = load_data()

if df.empty:
    st.warning("Nenhum dado encontrado nos arquivos processados.")
    st.stop()

col1, col2, col3 = st.columns(3)
with col1:
    municipios = df["municipio"].dropna().unique().tolist()
    all_label = "Todos"
    options_mun = [all_label] + municipios
    selected_municipios = st.multiselect("Município", options_mun, default=[all_label])

    if all_label in selected_municipios:
        municipio_sel = municipios
    else:
        municipio_sel = selected_municipios
with col2:
    anos = pd.to_datetime(df["data_inversa"]).dt.year.unique()
    ano_sel = st.multiselect("Ano", sorted(anos), default=list(anos))
with col3:
    mortos_opcao = st.selectbox(
        "Acidentes com mortos?",
        options=["Todos", "Apenas com mortos", "Apenas sem mortos"],
        index=0
    )

df_filt = df[
    df["municipio"].isin(municipio_sel) &
    (pd.to_datetime(df["data_inversa"]).dt.year.isin(ano_sel))
]

if mortos_opcao == "Apenas com mortos":
    df_filt = df_filt[df_filt["mortos"] == 1]
elif mortos_opcao == "Apenas sem mortos":
    df_filt = df_filt[df_filt["mortos"] == 0]

st.markdown("### Indicadores Gerais")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total de Acidentes", len(df_filt))
col2.metric("Total de Acidentes com Mortos", int(df_filt["mortos"].sum()))
col3.metric("Total de Feridos Graves", int(df_filt["feridos_graves"].sum()))
col4.metric("Municípios Envolvidos", df_filt["municipio"].nunique())

st.markdown("### Evolução dos Acidentes por Ano")
df_filt["ano"] = pd.to_datetime(df_filt["data_inversa"]).dt.year
st.line_chart(df_filt.groupby("ano").size())

st.markdown("### Top 10 Municípios com Mais Acidentes")
top_municipios = df_filt["municipio"].value_counts().head(10)
st.bar_chart(top_municipios)

if mortos_opcao == "Todos":
    st.markdown("### Proporção de Acidentes com e sem Mortos")
    mortos_counts = df_filt["mortos"].value_counts().sort_index()
    labels = ["Sem mortos", "Com mortos"]
    fig, ax = plt.subplots()
    ax.pie(mortos_counts, labels=labels, autopct='%1.1f%%', colors=["#66b3ff", "#ff6666"])
    ax.set_title("Proporção de Acidentes com e sem Mortos")
    st.pyplot(fig)

if "latitude" in df_filt.columns and "longitude" in df_filt.columns:
    st.markdown("### Mapa dos Acidentes")
    st.map(df_filt[["latitude", "longitude"]].dropna())

st.markdown("#### Dados filtrados")
if df_filt.empty:
    st.info("Nenhum dado encontrado para os filtros selecionados. Tente ampliar os filtros.")
else:
    st.dataframe(df_filt.head(100))

if st.button("Logout"):
    st.session_state.authenticated = False
    st.session_state.user = None
    st.rerun()