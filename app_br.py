import pandas as pd
import time
import os
import sys
from datetime import datetime
from riotwatcher import LolWatcher, ApiError
from sqlalchemy import create_engine, text

# --- CONFIGURAÇÃO ---
API_KEY = os.environ.get("RIOT_API_KEY") 
DB_URL = os.environ.get("DB_URL") 
REGION_MATCH = 'br1'      # Servidor das Partidas
REGION_ACCOUNT = 'americas' # Servidor de Contas (Riot ID)

# --- 🎯 LISTA DE JOGADORES ALVO (Edite aqui) ---
# Formato: "Nome#TAG"
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
engine = create_engine(DB_URL)

# --- REAPROVEITANDO A LÓGICA DE PROCESSAMENTO (IGUAL AO APP.PY) ---
def get_clean_version(version_str):
    parts = version_str.split('.')
    return f"{parts[0]}.{parts[1]}" if len(parts) >= 2 else version_str

def safe_div(a, b):
    return round(a / b, 2) if b != 0 else 0

def get_stats_at_minute(frames, minute, pid):
    if minute >= len(frames): return 0, 0, 0, 0
    frame = frames[minute]['participantFrames']
    pid_key = str(pid)
    if pid_key in frame:
        p = frame[pid_key]
        cs = p['minionsKilled'] + p['jungleMinionsKilled']
        return cs, p['totalGold'], p['xp'], p['level']
    return 0, 0, 0, 0

def get_team_total_at_minute(frames, minute, team_id, participants_info):
    if minute >= len(frames): return 1
    total_gold = 0
    frame = frames[minute]['participantFrames']
    team_pids = [str(p['participantId']) for p in participants_info if p['teamId'] == team_id]
    for pid in team_pids:
        if pid in frame: total_gold += frame[pid]['totalGold']
    return total_gold if total_gold > 0 else 1

def get_event_stats_at_minute(timeline_info, minute_limit, pid):
    kills = 0; deaths = 0; assists = 0; plates = 0
    limit_ms = minute_limit * 60 * 1000
    for frame in timeline_info['frames']:
        if frame['timestamp'] > limit_ms: break
        for event in frame['events']:
            if event['timestamp'] > limit_ms: continue
            if event['type'] == 'CHAMPION_KILL':
                if event.get('killerId') == pid: kills += 1
                if event.get('victimId') == pid: deaths += 1
                if pid in event.get('assistingParticipantIds', []): assists += 1
            if event['type'] == 'TURRET_PLATE_DESTROYED':
                if event.get('killerId') == pid or pid in event.get('assistingParticipantIds', []):
                    plates += 1
    return kills, deaths, assists, plates

def process_match(match_id):
    try:
        match = watcher.match.by_id(REGION_MATCH, match_id)
        timeline = watcher.match.timeline_by_match(REGION_MATCH, match_id)
    except Exception as err:
        print(f"Erro ao baixar partida {match_id}: {err}")
        return []

    info = match['info']
    duration_min = info['gameDuration'] / 60
    if duration_min < 15: return [] 
    
    patch = get_clean_version(info['gameVersion'])
    frames = timeline['info']['frames']
    start_time = info['gameCreation'] 

    role_map = {100: {}, 200: {}}
    p_info_dict = {}
    team_totals = {100: {'kills': 0, 'dmg': 0, 'taken': 0}, 200: {'kills': 0, 'dmg': 0, 'taken': 0}}

    for p in info['participants']:
        tid = p['teamId']
        team_totals[tid]['kills'] += p['kills']
        team_totals[tid]['dmg'] += p['totalDamageDealtToChampions']
        team_totals[tid]['taken'] += p['totalDamageTaken']
        p_info_dict[p['participantId']] = p
        if p.get('teamPosition'): role_map[tid][p['teamPosition']] = p['participantId']

    rows = []
    for p in info['participants']:
        pid = p['participantId']
        tid = p['teamId']
        pos = p['teamPosition']
        if not pos: continue

        enemy_team = 200 if tid == 100 else 100
        enemy_pid = role_map[enemy_team].get(pos)
        enemy_data = p_info_dict.get(enemy_pid) if enemy_pid else None
        if not enemy_data: continue

        stats = {
            'Qtd_Partidas': 1, 'Match ID': match_id, 'Patch': patch,
            'Champion': p['championName'], 'Enemy Champion': enemy_data['championName'],
            'Game Start Time': start_time, 'Win Rate %': 1 if p['win'] else 0,
            'Player Name': p['summonerName'], # Adicionado para identificar quem é quem
            'PUUID': p['puuid'],
            
            'Kills': p['kills'], 'Deaths': p['deaths'], 'Assists': p['assists'],
            'KDA': safe_div(p['kills'] + p['assists'], p['deaths']),
            'Total Damage Dealt': p['totalDamageDealtToChampions'],
            'Gold Earned': p['goldEarned'],
            'Farm/Min': safe_div(p['totalMinionsKilled'] + p['neutralMinionsKilled'], duration_min),
            'Damage/Min': safe_div(p['totalDamageDealtToChampions'], duration_min),
            
            'Vision Score': p['visionScore'],
            'Wards Placed': p['wardsPlaced'],
            
            'Damage to Buildings': p['damageDealtToBuildings'],
            'Turret Plates Taken': p.get('turretPlatesTaken', 0),
            
            'Team Damage %': safe_div(p['totalDamageDealtToChampions'], team_totals[tid]['dmg']),
            'Damage Taken %': safe_div(p['totalDamageTaken'], team_totals[tid]['taken']),
        }
        
        target_minutes = [5, 6, 11, 12, 14, 18, 20]
        for t in target_minutes:
            my_cs, my_gold, my_xp, my_lvl = get_stats_at_minute(frames, t, pid)
            en_cs, en_gold, en_xp, en_lvl = get_stats_at_minute(frames, t, enemy_pid)
            
            suffix = f"{t}'"
            if t in [14]: # Focando nos 14 min para economizar espaço
                stats[f'CS {suffix}'] = my_cs
                stats[f'Gold Diff {suffix}'] = my_gold - en_gold
                stats[f'CS Diff {suffix}'] = my_cs - en_cs
                stats[f'XP Diff {suffix}'] = my_xp - en_xp

        rows.append(stats)
    return rows

def get_puuids_from_names():
    player_data = []
    print("🔍 Buscando PUUIDs dos jogadores alvo...")
    for riot_id in ALVOS:
        try:
            if '#' not in riot_id:
                print(f"⚠️ Formato inválido: {riot_id} (Use Nome#TAG)")
                continue
            name, tag = riot_id.split('#')
            # Usa o endpoint de conta (Americas) para pegar PUUID
            account = watcher.account.by_riot_id(REGION_ACCOUNT, name, tag)
            player_data.append({'riot_id': riot_id, 'puuid': account['puuid']})
            print(f" > Encontrado: {riot_id}")
        except Exception as e:
            print(f"❌ Erro ao buscar {riot_id}: {e}")
        time.sleep(0.5)
    return player_data

def load_processed_ids():
    processed = set()
    try:
        # ATENÇÃO: Mudamos o nome da tabela para 'partidas_br'
        with engine.connect() as conn:
            query = text('SELECT "Match ID" FROM partidas_br')
            df_db = pd.read_sql(query, conn)
            processed.update(df_db['Match ID'].astype(str))
    except:
        pass
    return processed

def main():
    players = get_puuids_from_names()
    processed_ids = load_processed_ids()
    new_match_ids = set()
    
    # Coleta IDs de Partidas dos Jogadores
    print("\n🔍 Buscando partidas recentes...")
    for p in players:
        try:
            # Pega as últimas 10 partidas de cada um
            matches = watcher.match.matchlist_by_puuid(REGION_MATCH, p['puuid'], count=10)
            for m in matches:
                if m not in processed_ids:
                    new_match_ids.add(m)
        except Exception as e:
            print(f"Erro ao pegar lista de {p['riot_id']}: {e}")
        time.sleep(1)
        
    match_list = list(new_match_ids)
    if not match_list:
        print("Nenhuma partida nova para processar.")
        return

    print(f"\n📥 Baixando detalhes de {len(match_list)} partidas novas...")
    buffer = []
    for i, m_id in enumerate(match_list):
        data = process_match(m_id)
        if data: buffer.extend(data)
        time.sleep(1.2) # Respeitando rate limit

    if buffer:
        df_new = pd.DataFrame(buffer)
        
        # MUDANÇA IMPORTANTE: Salvando na tabela 'partidas_br'
        print("💾 Salvando na tabela 'partidas_br'...")
        df_new.to_sql('partidas_br', engine, if_exists='append', index=False, chunksize=500)
        print("✅ SUCESSO! Dados BR salvos.")

if __name__ == "__main__":
    main()
