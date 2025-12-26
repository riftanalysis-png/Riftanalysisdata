import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os

# --- CONFIGURAÇÃO ---
st.set_page_config(page_title="LoL Matchup Pro", layout="wide", page_icon="🥊")

# --- CONEXÃO E DADOS ---
def get_engine():
    db_url = st.secrets.get("DB_URL") or os.environ.get("DB_URL")
    if not db_url:
        st.error("⚠️ Sem conexão com o Banco.")
        st.stop()
    return create_engine(db_url)

@st.cache_data(ttl=600)
def load_data():
    try:
        engine = get_engine()
        # Ordenado por data para pegar o meta atual
        query = 'SELECT * FROM partidas ORDER BY "Game Start Time" DESC'
        df = pd.read_sql(query, engine)
        
        # Tratamentos
        df['Game Start Time'] = pd.to_datetime(df['Game Start Time'], unit='ms')
        df['Data'] = df['Game Start Time'].dt.strftime('%d/%m/%Y')
        df['Resultado'] = df['Win Rate %'].apply(lambda x: '✅ Win' if x == 1 else '❌ Loss')
        
        return df
    except Exception as e:
        st.error(f"Erro: {e}")
        return pd.DataFrame()

df = load_data()

if df.empty:
    st.warning("Banco de dados vazio. Aguarde a coleta.")
    st.stop()

# --- SIDEBAR GLOBAL ---
st.sidebar.header("🎯 Seleção do Jogador")
# Se tivermos mais de um jogador monitorado no futuro, filtramos aqui. 
# Por enquanto, assumimos que todos os dados são relevantes.
periodo = st.sidebar.selectbox("Período", ["Todos os Tempos", "Últimos 30 dias", "Season Atual"])

# --- ESTRUTURA DE ABAS ---
st.title("🥊 Análise de Matchups (SoloQ)")
tab_matchups, tab_geral, tab_raw = st.tabs(["⚔️ Matchups Específicos", "📊 Performance Geral", "📝 Dados Brutos"])

# ==============================================================================
# ABA 1: MATCHUP ANALYZER (O FOCO AGORA)
# ==============================================================================
with tab_matchups:
    col_sel1, col_sel2 = st.columns(2)
    
    with col_sel1:
        # Escolha SEU campeão
        meus_champs = sorted(df['Champion'].unique())
        meu_champ = st.selectbox("Eu estou jogando de:", meus_champs)
    
    # Filtra dados apenas desse campeão
    df_meu = df[df['Champion'] == meu_champ]
    
    if df_meu.empty:
        st.info("Sem dados para este campeão.")
    else:
        # Agrupa por INIMIGO para ver contra quem jogamos
        # Calculamos MÉDIA e MEDIANA para comparar
        matchup_stats = df_meu.groupby('Enemy Champion').agg({
            'Match ID': 'count',
            'Win Rate %': 'mean',
            "Gold Diff 14'": ['mean', 'median'],
            "CS Diff 14'": ['mean', 'median'],
            "XP Diff 14'": ['mean'],
            'Kills': 'mean',
            'Deaths': 'mean'
        }).reset_index()

        # Ajustando nomes das colunas (O Pandas cria MultiIndex, vamos achatar)
        matchup_stats.columns = [
            'Inimigo', 'Jogos', 'Win Rate', 
            'Gold Diff (Média)', 'Gold Diff (Mediana)',
            'CS Diff (Média)', 'CS Diff (Mediana)',
            'XP Diff (Média)', 'Kills', 'Deaths'
        ]
        
        # Filtra apenas matchups com pelo menos 1 jogo (pode aumentar depois)
        matchup_stats = matchup_stats[matchup_stats['Jogos'] > 0].sort_values(by='Jogos', ascending=False)

        # KPI DO CAMPEÃO GERAL
        st.divider()
        c1, c2, c3, c4 = st.columns(4)
        c1.metric(f"Jogos de {meu_champ}", len(df_meu))
        c2.metric("Win Rate Global", f"{df_meu['Win Rate %'].mean()*100:.1f}%")
        c3.metric("GD@15 Médio", f"{df_meu["Gold Diff 14'"].mean():.0f}")
        c4.metric("KDA Médio", f"{df_meu['KDA'].mean():.2f}")
        st.divider()

        with col_sel2:
            # Escolha o INIMIGO específico para detalhar
            inimigos_disponiveis = sorted(matchup_stats['Inimigo'].unique())
            inimigo_foco = st.selectbox("Contra o Inimigo (Detalhar):", ["Todos"] + inimigos_disponiveis)

        # VISUALIZAÇÃO 1: TABELA GERAL DE MATCHUPS
        if inimigo_foco == "Todos":
            st.subheader(f"Como {meu_champ} lida com cada matchup?")
            st.caption("Ordenado por popularidade. GD@14 = Diferença de Ouro aos 14 min.")
            
            # Formatação visual da tabela
            st.dataframe(
                matchup_stats,
                column_config={
                    "Win Rate": st.column_config.ProgressColumn("Win Rate", format="%.1f%%", min_value=0, max_value=1),
                    "Gold Diff (Mediana)": st.column_config.NumberColumn("Ouro @14 (Mediana)", format="%d"),
                    "CS Diff (Mediana)": st.column_config.NumberColumn("CS @14 (Mediana)", format="%d"),
                },
                hide_index=True,
                use_container_width=True
            )
            
            # Gráfico de Bolhas: Dificuldade do Matchup
            # Eixo X: Gold Diff (Laning Phase) | Eixo Y: Win Rate (Jogo) | Tamanho: Nº Jogos
            st.subheader("Mapa de Dificuldade")
            st.caption("Esquerda/Baixo = Matchups Difíceis | Direita/Cima = Matchups Fáceis")
            st.scatter_chart(
                matchup_stats,
                x='Gold Diff (Mediana)',
                y='Win Rate',
                size='Jogos',
                color='Inimigo' # Cada bolinha é um boneco inimigo
            )

        # VISUALIZAÇÃO 2: X1 ESPECÍFICO (Drill Down)
        else:
            st.subheader(f"⚔️ Análise Detalhada: {meu_champ} vs {inimigo_foco}")
            
            df_x1 = df_meu[df_meu['Enemy Champion'] == inimigo_foco]
            
            # Comparação Lado a Lado
            k1, k2, k3 = st.columns(3)
            media_gd = df_x1["Gold Diff 14'"].mean()
            k1.metric("Jogos Analisados", len(df_x1))
            k2.metric("Win Rate no Matchup", f"{df_x1['Win Rate %'].mean()*100:.0f}%")
            k3.metric(
                "Ouro @ 14min", 
                f"{media_gd:.0f}", 
                delta=f"{media_gd - df_meu["Gold Diff 14'"].mean():.0f} vs Média Geral",
                delta_color="normal" # Verde se for melhor que a sua média normal
            )
            
            st.write("Histórico desse confronto:")
            st.dataframe(df_x1[['Data', 'Resultado', 'KDA', "Gold Diff 14'", "CS Diff 14'", "XP Diff 14'"]])

# ==============================================================================
# ABA 2: DADOS GERAIS (Backup da versão anterior)
# ==============================================================================
with tab_geral:
    st.subheader("Performance Geral da Conta")
    champion_stats = df.groupby('Champion').agg({'Match ID':'count', 'Win Rate %':'mean', 'KDA':'mean'}).reset_index()
    champion_stats = champion_stats.sort_values('Match ID', ascending=False)
    st.dataframe(champion_stats, hide_index=True, use_container_width=True)

# ==============================================================================
# ABA 3: RAW DATA
# ==============================================================================
with tab_raw:
    st.dataframe(df)
