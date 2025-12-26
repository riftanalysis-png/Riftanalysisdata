import pandas as pd
import time
import os
import sys
from datetime import datetime
from riotwatcher import LolWatcher, RiotWatcher, ApiError
from sqlalchemy import create_engine, text

# --- CONFIGURAÇÃO ---
# O Workflow do GitHub mapeia a secret RIOT_API_KEY_BR para esta variável
API_KEY = os.environ.get("RIOT_API_KEY") 
DB_URL = os.environ.get("DB_URL") 
REGION_MATCH = 'br1'
REGION_ACCOUNT = 'americas'

# --- 🎯 SEUS JOGADORES ALVO ---
ALVOS = [
    "Zekas#2002",
    "han dao#EGC",
    "Pilot#br11",
    "Celo#br2",
    "Gatovisck#愛憎の影"
]

sys.stdout.reconfigure(line_buffering=True)

if not API_KEY or not DB_URL:
    print("ERRO: Credenciais ausentes.")
    sys.exit(1)

watcher = LolWatcher(API_KEY)
riot_watcher = RiotWatcher(API_KEY)
engine = create_engine(DB_URL)

# --- FUNÇÕES AUXILIARES MATEMÁTICAS ---
def get_clean_version(version_str):
    parts = version_str.split('.')
    return f"{parts[0]}.{parts[1]}" if len(parts) >= 2 else version_str

def safe_div(a, b):
    return round(a / b, 2) if b != 0 else 0

def get_snapshot_at_minute(frames, minute, pid, team_id, participants_info):
    """Retorna estatísticas acumuladas até o minuto X."""
    if minute >= len(frames): 
        return None
    
    frame = frames[minute]['participantFrames'].get(str(pid))
    if not frame: return None
    
    # Coleta de métricas básicas do frame
    cs = frame['minionsKilled'] + frame['jungleMinionsKilled']
    gold = frame['totalGold']
    xp = frame['xp']
    damage = frame.get('damageStats', {}).get('totalDamageDoneToChampions', 0)
    
    # Cálculo do total do time para Gold Share
    team_gold = 0
    team_pids = [str(p['participantId']) for p in participants_info if p['teamId'] == team_id]
    for t_pid in team_pids:
        t_frame = frames[minute]['participantFrames'].get(t_pid)
        if t_frame: team_gold += t_frame['totalGold']
        
    gold_share = safe_div(gold, team_gold)
    
    return {
        'cs': cs, 'gold': gold, 'xp': xp, 'damage': damage, 
        'level': frame['level'], 'gold_share': gold_share
    }

def get_events_at_minute(timeline_info, minute, pid):
    """Conta Kills, Deaths, Assists, Plates até o minuto X."""
    limit_ms = minute * 60 * 1000
    kills = 0; deaths = 0; assists = 0; plates = 0
    
    for frame in timeline_info['info']['frames']:
        if frame['timestamp'] > limit_ms: break
        for event in frame['events']:
            # Pula eventos futuros
            if event['timestamp'] > limit_ms: continue
            
            # K/D/A
            if event['type'] == 'CHAMPION_KILL':
                if event.get('killerId') == pid: kills += 1
                if event.get('victimId') == pid: deaths += 1
                if pid in event.get('assistingParticipantIds', []): assists += 1
            
            # Barricadas
            if event['type'] == 'TURRET_PLATE_DESTROYED':
                if event.get('killerId') == pid or pid in event.get('assistingParticipantIds', []):
                    plates += 1
                    
    return {'kills': kills, 'deaths': deaths, 'assists': assists, 'plates': plates}

# --- PROCESSAMENTO PRINCIPAL ---
def process_match(match_id):
    try:
        match = watcher.match.by_id(REGION_MATCH, match_id)
        timeline = watcher.match.timeline_by_match(REGION_MATCH, match_id)
    except Exception as err:
        print(f"Erro ao baixar partida {match_id}: {err}")
        return []

    info = match['info']
    duration_min = info['gameDuration'] / 60
    if duration_min < 15: return [] # Ignora remakes
    
    patch = get_clean_version(info['gameVersion'])
    frames = timeline['info']['frames']
    start_time = info['gameCreation'] 

    # Mapa de posições para achar o oponente direto
    role_map = {100: {}, 200: {}}
    participants_info = info['participants']
    
    # Pré-cálculos de totais do time
    team_totals = {100: {'dmg': 0, 'taken': 0}, 200: {'dmg': 0, 'taken': 0}}

    for p in participants_info:
        tid = p['teamId']
        team_totals[tid]['dmg'] += p['totalDamageDealtToChampions']
        team_totals[tid]['taken'] += p['totalDamageTaken']
        if p.get('teamPosition'): 
            role_map[tid][p['teamPosition']] = p['participantId']

    rows = []
    for p in participants_info:
        pid = p['participantId']
        tid = p['teamId']
        pos = p['teamPosition']
        
        # Só processa se tiver posição definida (ignora casos bugados)
        if not pos: continue

        enemy_team = 200 if tid == 100 else 100
        enemy_pid = role_map[enemy_team].get(pos)
        
        # Pega nome do inimigo
        enemy_name = "Desconhecido"
        enemy_champ = "None"
        if enemy_pid:
            enemy_data = next((x for x in participants_info if x['participantId'] == enemy_pid), None)
            if enemy_data:
                enemy_champ = enemy_data['championName']

        # --- ESTATÍSTICAS BASE (Fim de Jogo) ---
        stats = {
            'Qtd_Partidas': 1, 
            'Match ID': match_id, 
            'Patch': patch,
            'Champion': p['championName'], 
            'Enemy Champion': enemy_champ,
            'Game Start Time': start_time, 
            'Win Rate %': 1 if p['win'] else 0,
            
            # Identificação
            'Player Name': p['summonerName'], 
            'PUUID': p['puuid'],

            # KDA & Combate
            'Kills': p['kills'], 
            'Deaths': p['deaths'], 
            'Assists': p['assists'],
            'KDA': safe_div(p['kills'] + p['assists'], p['deaths']),
            'Kill Participation': safe_div(p['kills'] + p['assists'], info['teams'][0]['objectives']['champion']['kills'] if tid==100 else info['teams'][1]['objectives']['champion']['kills']),
            'Total Damage Dealt': p['totalDamageDealtToChampions'],
            'Total Damage Taken': p['totalDamageTaken'],
            'Self Mitigated Damage': p['totalDamageSelfMitigated'],
            
            # Economia
            'Gold Earned': p['goldEarned'],
            'Farm/Min': safe_div(p['totalMinionsKilled'] + p['neutralMinionsKilled'], duration_min),
            'Damage/Min': safe_div(p['totalDamageDealtToChampions'], duration_min),
            'Gold/Min': safe_div(p['goldEarned'], duration_min),
            
            # Visão
            'Vision Score': p['visionScore'],
            'Vision Score/Min': safe_div(p['visionScore'], duration_min),
            'Wards Placed': p['wardsPlaced'],
            'Wards Killed': p['wardsKilled'],
            'Control Wards Placed': p['detectorWardsPlaced'],
            
            # Objetivos
            'Damage to Buildings': p['damageDealtToBuildings'],
            'Damage to Objectives': p['damageDealtToObjectives'],
            'Turret Plates Taken': p.get('turretPlatesTaken', 0),
            
            # Percentuais
            'Team Damage %': safe_div(p['totalDamageDealtToChampions'], team_totals[tid]['dmg']),
            'Damage Taken %': safe_div(p['totalDamageTaken'], team_totals[tid]['taken']),
            
            # Extras
            'First Blood Kill': 1 if p.get('firstBloodKill') else 0,
            'First Blood Assist': 1 if p.get('firstBloodAssist') else 0,
            'First Tower Kill': 1 if p.get('firstTowerKill') else 0,
            'First Tower Assist': 1 if p.get('firstTowerAssist') else 0,
            'CC Score': p['timeCCingOthers']
        }
        
        # --- TIMELINE LOOP (5, 6, 11, 12, 14, 18, 20 min) ---
        # Lista de minutos que você pediu
        minutes_to_check = [5, 6, 11, 12, 14, 18, 20]
        
        for t in minutes_to_check:
            # Pega dados meus e do inimigo naquele minuto
            my_snap = get_snapshot_at_minute(frames, t, pid, tid, participants_info)
            en_snap = get_snapshot_at_minute(frames, t, enemy_pid, enemy_team, participants_info) if enemy_pid else None
            my_events = get_events_at_minute(timeline, t, pid)
            
            if my_snap:
                suffix = f"{t}'" # Ex: 5'
                
                # Campos "Padrão" para 5, 11, 12, 14, 20
                if t in [5, 11, 12, 14, 20]:
                    stats[f"Kills {suffix}"] = my_events['kills']
                    stats[f"Deaths {suffix}"] = my_events['deaths']
                    stats[f"Assists {suffix}"] = my_events['assists']
                    stats[f"CS {suffix}"] = my_snap['cs']
                    stats[f"Gold Earned {suffix}"] = my_snap['gold']
                    stats[f"Plates {suffix}"] = my_events['plates']
                    stats[f"KDA {suffix}"] = safe_div(my_events['kills'] + my_events['assists'], my_events['deaths'])
                    stats[f"GPM {suffix}"] = safe_div(my_snap['gold'], t)
                    stats[f"DPM {suffix}"] = safe_div(my_snap['damage'], t)
                    stats[f"Gold Share {suffix}"] = my_snap['gold_share']
                    # Gold Eff = Dano / Ouro (Eficiência de conversão de recurso em impacto)
                    stats[f"Gold Eff {suffix}"] = safe_div(my_snap['damage'], my_snap['gold'])
                    
                    # Diffs
                    if en_snap:
                        stats[f"CS Diff {suffix}"] = my_snap['cs'] - en_snap['cs']
                        stats[f"Gold Diff {suffix}"] = my_snap['gold'] - en_snap['gold']
                        stats[f"XP Diff {suffix}"] = my_snap['xp'] - en_snap['xp']
                        stats[f"DMG Diff {suffix}"] = my_snap['damage'] - en_snap['damage']
                    else:
                        stats[f"CS Diff {suffix}"] = 0
                        stats[f"Gold Diff {suffix}"] = 0
                        stats[f"XP Diff {suffix}"] = 0
                        stats[f"DMG Diff {suffix}"] = 0

                # Campos Específicos/Redundantes que você pediu na lista
                if t == 6:
                    stats["CS aos 6 min"] = my_snap['cs']
                
                if t == 12:
                    stats["CS aos 12 min"] = my_snap['cs']
                    stats["Gold aos 12 min"] = my_snap['gold']
                    stats["XP aos 12 min"] = my_snap['xp']
                    stats["Deaths até 12min"] = my_events['deaths']
                    stats["VPM @12"] = safe_div(p['visionScore'], 12) # Estimativa linear
                    stats["KDA @12"] = safe_div(my_events['kills'] + my_events['assists'], my_events['deaths'])

                if t == 18:
                    stats["CS aos 18 min"] = my_snap['cs']

        rows.append(stats)
    return rows

def get_puuids_from_names():
    player_data = []
    print("🔍 Buscando PUUIDs dos jogadores alvo...")
    for riot_id in ALVOS:
        try:
            if '#' not in riot_id:
                print(f"⚠️ Formato inválido: {riot_id}")
                continue
            name, tag = riot_id.split('#')
            account = riot_watcher.account.by_riot_id(REGION_ACCOUNT, name, tag)
            player_data.append({'riot_id': riot_id, 'puuid': account['puuid']})
            print(f" > Encontrado: {riot_id}")
        except Exception as e:
            print(f"❌ Erro ao buscar {riot_id}: {e}")
        time.sleep(0.5)
    return player_data

def load_processed_ids():
    processed = set()
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1 FROM partidas_br LIMIT 1"))
            query = text('SELECT "Match ID" FROM partidas_br')
            df_db = pd.read_sql(query, conn)
            processed.update(df_db['Match ID'].astype(str))
            print(f"Histórico BR carregado: {len(processed)} partidas.")
    except Exception:
        print("Tabela 'partidas_br' será criada do zero.")
    return processed

def main():
    players = get_puuids_from_names()
    if not players: return

    processed_ids = load_processed_ids()
    new_match_ids = set()
    
    print("\n🔍 Buscando partidas recentes...")
    for p in players:
        try:
            matches = watcher.match.matchlist_by_puuid(REGION_MATCH, p['puuid'], count=10)
            for m in matches:
                if m not in processed_ids:
                    new_match_ids.add(m)
        except Exception as e:
            print(f"Erro lista {p['riot_id']}: {e}")
        time.sleep(1)
        
    match_list = list(new_match_ids)
    if not match_list:
        print("Nenhuma partida nova.")
        return

    print(f"\n📥 Baixando {len(match_list)} partidas (Modo Lento)...")
    buffer = []
    for i, m_id in enumerate(match_list):
        data = process_match(m_id)
        if data: buffer.extend(data)
        
        # ⚠️ DELAY AUMENTADO PARA EVITAR ERRO 429
        time.sleep(3.5) 
        print(f" [{i+1}/{len(match_list)}] Processado...")

    if buffer:
        df_new = pd.DataFrame(buffer)
        print("💾 Salvando na tabela 'partidas_br'...")
        try:
            # Salva no Supabase (cria colunas novas automaticamente)
            df_new.to_sql('partidas_br', engine, if_exists='append', index=False, chunksize=500)
            print("✅ SUCESSO! Dados completos salvos.")
        except Exception as e:
            print(f"❌ Erro de Banco: {e}")

if __name__ == "__main__":
    main()
