import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="LoL Data Analysis", layout="wide")

# Título e Estilo
st.title("⚔️ Rift Analysis Dashboard")
st.markdown("Monitoramento de performance na SoloQ Coreana")

# --- CONEXÃO COM O BANCO ---
# O Streamlit Cloud usa 'st.secrets' para guardar senhas
try:
    if "DB_URL" in st.secrets:
        DB_URL = st.secrets["DB_URL"]
    else:
        DB_URL = os.environ.get("DB_URL") # Fallback para local

    if not DB_URL:
        st.error("ERRO: URL do Banco não encontrada nos Segredos.")
        st.stop()
        
    engine = create_engine(DB_URL)
except Exception as e:
    st.error(f"Erro de Configuração: {e}")
    st.stop()

# --- CARREGAMENTO DE DADOS (COM CACHE) ---
@st.cache_data(ttl=600) # Guarda na memória por 10 min para ser rápido
def load_data():
    try:
        # Lê a tabela inteira (se ficar muito grande no futuro, limitamos aqui)
        query = 'SELECT * FROM partidas'
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Erro ao ler do banco: {e}")
        return pd.DataFrame()

with st.spinner('Carregando dados do Supabase...'):
    df = load_data()

if df.empty:
    st.warning("Ainda não há dados na tabela. Espere o robô rodar amanhã!")
    st.stop()

# --- FILTROS LATERAIS ---
st.sidebar.header("Filtros")
lista_campeoes = sorted(df['Champion'].unique())
campeao_selecionado = st.sidebar.multiselect("Filtrar por Campeão", lista_campeoes)

if campeao_selecionado:
    df_filtered = df[df['Champion'].isin(campeao_selecionado)]
else:
    df_filtered = df

# --- KPIs (MÉTRICAS PRINCIPAIS) ---
col1, col2, col3, col4 = st.columns(4)
col1.metric("Partidas Analisadas", len(df_filtered))
col2.metric("Win Rate", f"{df_filtered['Win Rate %'].mean()*100:.1f}%")
col3.metric("KDA Médio", f"{df_filtered['KDA'].mean():.2f}")
col4.metric("Farm/Min", f"{df_filtered['Farm/Min'].mean():.1f}")

# --- GRÁFICOS ---
st.divider()

col_g1, col_g2 = st.columns(2)

with col_g1:
    st.subheader("💰 Diferença de Ouro aos 14' (Early Game)")
    st.line_chart(df_filtered, x='Game Start Time', y="Gold Diff 14'")

with col_g2:
    st.subheader("📊 Dano Causado vs. Ouro (Eficiência)")
    # Gráfico de Dispersão: Quem farma muito e bate pouco?
    st.scatter_chart(
        df_filtered,
        x='Gold Earned',
        y='Total Damage Dealt',
        color='Champion' if campeao_selecionado else 'Win Rate %'
    )

# Tabela Detalhada
with st.expander("Ver Dados Brutos"):
    st.dataframe(df_filtered)
