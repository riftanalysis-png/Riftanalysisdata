import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
import os

# --- CONFIGURAÇÃO VISUAL ---
st.set_page_config(page_title="Pro Player Profile", layout="wide", page_icon="🛡️")

# CSS para tentar imitar o visual "Dark Blue" das imagens
st.markdown("""
<style>
    .stMetric {
        background-color: #0e1117;
        padding: 10px;
        border-radius: 5px;
        border: 1px solid #303030;
    }
    h1, h2, h3 {
        color: #f0f2f6;
    }
    .big-font {
        font-size:20px !important;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# --- CONEXÃO ---
def get_engine():
    db_url = st.secrets.get("DB_URL") or os.environ.get("DB_URL")
    return create_engine(db_url)

@st.cache_data(ttl=600)
def load_data():
    try:
        engine = get_engine()
        # Puxa da tabela BR agora
        query = 'SELECT * FROM partidas_br ORDER BY "Game Start Time" DESC'
        df = pd.read_sql(query, engine)
        df['Game Start Time'] = pd.to_datetime(df['Game Start Time'], unit='ms')
        df['Data'] = df['Game Start Time'].dt.strftime('%d/%m')
        return df
    except Exception as e:
        return pd.DataFrame()

df_raw = load_data()

if df_raw.empty:
    st.error("Sem dados na tabela 'partidas_br'. Rode o coletor primeiro!")
    st.stop()

# --- SIDEBAR: SELEÇÃO DO JOGADOR ---
st.sidebar.header("🕵️ Seleção de Perfil")
lista_jogadores = df_raw['Player Name'].unique()
jogador = st.sidebar.selectbox("Escolha o Pro Player:", lista_jogadores)

# Filtra dados do jogador
df = df_raw[df_raw['Player Name'] == jogador].copy()

# --- CÁLCULO DE "NOTAS" (SCOUTING) ---
# Heurística simples para gerar notas de 0 a 100 baseadas nas stats
media_lane = df["Gold Diff 14'"].mean()
nota_lane = min(100, max(50, 70 + (media_lane / 50))) # Base 70, sobe/desce conforme ouro

media_kp = df['Kill Participation'].mean() * 100
nota_impacto = min(100, max(40, media_kp + 20)) 

media_dpm = df['Damage/Min'].mean()
nota_conversao = min(100, max(50, (media_dpm / 1000) * 100))

media_visao = df['Vision Score/Min'].mean()
nota_visao = min(100, max(40, media_visao * 40))

nota_geral = (nota_lane + nota_impacto + nota_conversao + nota_visao) / 4

# --- LAYOUT PRINCIPAL (GRID) ---
st.title(f"📊 Relatório de Scout: {jogador}")
st.divider()

# Cria 3 colunas principais (Esquerda, Centro, Direita)
col_L, col_C, col_R = st.columns([1.2, 0.8, 1.5])

# --- COLUNA ESQUERDA: CAMPEÕES E GRÁFICOS ---
with col_L:
    st.subheader("🏆 Melhores Campeões")
    stats_champ = df.groupby('Champion').agg({
        'Match ID': 'count',
        'Win Rate %': 'mean',
        'KDA': 'mean'
    }).reset_index()
    stats_champ.columns = ['Champ', 'Jogos', 'Win Rate', 'KDA']
    stats_champ = stats_champ.sort_values('Jogos', ascending=False).head(5)
    
    st.dataframe(
        stats_champ, 
        hide_index=True,
        column_config={
            "Win Rate": st.column_config.ProgressColumn("WR", format="%.0f%%", min_value=0, max_value=1),
            "KDA": st.column_config.NumberColumn("KDA", format="%.2f")
        },
        use_container_width=True
    )
    
    st.subheader("📉 Eficiência de Recursos")
    # Bolhas: Eixo X = Ouro, Eixo Y = Dano, Tamanho = KDA
    fig_bubble = px.scatter(
        df, 
        x="Gold/Min", 
        y="Damage/Min", 
        size="KDA", 
        color="Win Rate %",
        hover_data=['Champion'],
        color_continuous_scale=["red", "green"],
        title="Dano por Ouro (Quem carrega mais?)"
    )
    fig_bubble.update_layout(height=300, margin=dict(l=0, r=0, t=30, b=0))
    st.plotly_chart(fig_bubble, use_container_width=True)

# --- COLUNA CENTRAL: CARD DO JOGADOR ---
with col_C:
    st.markdown(f"<div style='text-align: center;'><h2>{jogador}</h2></div>", unsafe_allow_html=True)
    
    # Simulação da foto (Usamos o icone do campeão mais jogado ou algo genérico)
    main_champ = stats_champ.iloc[0]['Champ']
    st.image(f"https://ddragon.leagueoflegends.com/cdn/img/champion/splash/{main_champ}_0.jpg", caption="Main Pick")
    
    # As notas estilo FIFA/Scout
    c1, c2 = st.columns(2)
    c1.metric("Lane Phase", f"{nota_lane:.0f}")
    c2.metric("Impacto", f"{nota_impacto:.0f}")
    
    c3, c4 = st.columns(2)
    c3.metric("Conversão", f"{nota_conversao:.0f}")
    c4.metric("Visão", f"{nota_visao:.0f}")
    
    st.markdown(f"<h1 style='text-align: center; color: gold; font-size: 60px;'>{nota_geral:.0f}</h1>", unsafe_allow_html=True)
    st.markdown("<div style='text-align: center;'>OVERALL SCORE</div>", unsafe_allow_html=True)

# --- COLUNA DIREITA: TABELA DENSA DE KPIS ---
with col_R:
    st.subheader("📑 Estatísticas Detalhadas")
    
    # Função para criar linhas da tabela bonitinha
    def kpi_row(label, value, fmt="{:.2f}"):
        return f"**{label}:** {fmt.format(value)}"

    # Organizando em grid 3x3
    k1, k2, k3 = st.columns(3)
    k1.metric("KDA", f"{df['KDA'].mean():.2f}")
    k2.metric("Visão/Min", f"{df['Vision Score/Min'].mean():.2f}")
    k3.metric("DPM", f"{df['Damage/Min'].mean():.0f}")
    
    k4, k5, k6 = st.columns(3)
    k4.metric("GPM", f"{df['Gold/Min'].mean():.0f}")
    k5.metric("Kill Part", f"{df['Kill Participation'].mean()*100:.1f}%")
    k6.metric("Farm/Min", f"{df['Farm/Min'].mean():.1f}")

    st.divider()
    
    st.markdown("#### ⚔️ Lane Phase (14 min)")
    l1, l2, l3 = st.columns(3)
    
    # Tenta pegar as colunas de 14 min (usando .get para não quebrar se não existir)
    cs_diff = df.get("CS Diff 14'", pd.Series([0])).mean()
    gold_diff = df.get("Gold Diff 14'", pd.Series([0])).mean()
    xp_diff = df.get("XP Diff 14'", pd.Series([0])).mean()
    
    l1.metric("CS Diff", f"{cs_diff:+.1f}", delta_color="normal")
    l2.metric("Gold Diff", f"{gold_diff:+.0f}", delta_color="normal")
    l3.metric("XP Diff", f"{xp_diff:+.0f}", delta_color="normal")

    # Gráfico de Desempenho (Linha)
    st.subheader("📈 Evolução de Ouro")
    st.line_chart(df.set_index("Data")["Gold Diff 14'"], height=200)

st.divider()

# --- ROW 2: RADAR CHARTS (ESTILO DE JOGO) ---
st.subheader("🕸️ Análise de Estilo (Radar Charts)")
r1, r2, r3 = st.columns(3)

def create_radar(categories, values, title):
    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=values,
        theta=categories,
        fill='toself',
        name=jogador,
        line_color='#00ffcc'
    ))
    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        showlegend=False,
        title=title,
        height=300,
        margin=dict(l=40, r=40, t=40, b=20)
    )
    return fig

with r1:
    # Game Style (Simulado com heurísticas)
    agg = df['Win Rate %'].mean() * 100
    snowball = min(100, max(0, gold_diff / 20 + 50))
    farm = min(100, (df['Farm/Min'].mean() / 10) * 100)
    cats = ['Team Fight', 'Snowball', 'Farm', 'Vision', 'Aggression']
    vals = [nota_impacto, snowball, farm, nota_visao, agg]
    st.plotly_chart(create_radar(cats, vals, "Estilo de Jogo"), use_container_width=True)

with r2:
    # Lane Dominance
    # Normalizando diffs para escala 0-100 (Assumindo que +1000 gold = 100, -1000 = 0)
    gd_norm = min(100, max(0, 50 + (gold_diff / 40)))
    xp_norm = min(100, max(0, 50 + (xp_diff / 40)))
    cs_norm = min(100, max(0, 50 + (cs_diff / 2)))
    solo_kill = min(100, df['Kills'].mean() * 20) # Exemplo
    
    cats_lane = ['Gold @14', 'XP @14', 'CS @14', 'Plates', 'Solo Kills']
    vals_lane = [gd_norm, xp_norm, cs_norm, 60, solo_kill] # Plates hardcoded pq nao temos diff
    st.plotly_chart(create_radar(cats_lane, vals_lane, "Dominância de Rota"), use_container_width=True)

with r3:
    # Correlações (Scatter simples)
    st.markdown("#### 🎯 Foco do Treino")
    st.write("Relação: **Farm aos 14** vs **XP Diff**")
    fig_corr = px.scatter(df, x="CS 14'", y="XP Diff 14'", trendline="ols", color="Resultado")
    st.plotly_chart(fig_corr, use_container_width=True)
