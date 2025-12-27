import pandas as pd
import time
import os
import sys
from datetime import datetime
from riotwatcher import LolWatcher, RiotWatcher, ApiError
from sqlalchemy import create_engine, text
from sqlalchemy import inspect 

# --- CONFIGURAÇÃO ---
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

# --- FUNÇÕES AUXILIARES ---
def get_clean_version(version_str):
    parts = version_str.split('.')
    return f"{parts[0]}.{parts[1]}" if len(parts) >= 2 else version_str

def safe_div(a, b):
    return round(a / b, 2) if b != 0 else 0

def get_snapshot_at_minute(frames, minute, pid, team_id, participants_info):
    if minute >= len(frames): return None
    frame = frames[minute]['participantFrames'].get(str(pid))
    if not frame: return None
    
    cs = frame['minionsKilled'] + frame['jungleMinionsKilled']
    gold = frame['totalGold']
    xp = frame['xp']
    damage = frame.get('damageStats', {}).get('totalDamageDoneToChampions', 0)
    
    team_gold = 0
    team_pids = [str(p['participantId']) for p in participants_info if p['teamId'] == team_id]
    for t_pid in team_pids:
        t_frame = frames[minute]['participantFrames'].get(t_pid)
        if t_frame: team_gold += t_frame['totalGold']
        
    gold_share = safe_div(gold, team_gold)
    
    return {'cs': cs, 'gold': gold, 'xp': xp, 'damage': damage, 'level': frame['level'], 'gold_share': gold_share}

def get_events_at_minute(timeline_info, minute, pid):
    limit_ms = minute * 60 * 1000
    kills = 0; deaths = 0; assists = 0; plates = 0
    for frame in timeline_info['info']['frames']:
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
    return {'kills': kills, 'deaths': deaths, 'assists': assists, 'plates': plates}

def process_match(match_id):
    try:
        match = watcher.match.by_id(REGION_MATCH, match_id)
        timeline = watcher.match.timeline_by_match(REGION_MATCH, match_id)
    except Exception as err:
        print(f"Erro ao baixar partida {match_id}: {err}")
        return []

    info = match['info']
    duration_seconds = info['gameDuration']
    duration_min = duration_seconds / 60
    
    # 🛡️ FILTRO DE REMAKE (< 3min 30s)
    if duration_seconds < 210: 
        print(f" ⏩ Ignorando Remake ({duration_min:.1f} min)")
        return [] 
    
    patch = get_clean_version(info['gameVersion'])
    frames = timeline['info']['frames']
    start_time = info['gameCreation'] 
    
    role_map = {100: {}, 200: {}}
    participants_info = info['participants']
    team_totals = {100: {'dmg': 0, 'taken': 0}, 200: {'dmg': 0, 'taken': 0}}

    for p in participants_info:
        tid = p['teamId']
        team_totals[tid]['dmg'] += p['totalDamageDealtToChampions']
        team_totals[tid]['taken'] += p['totalDamageTaken']
        if p.get('teamPosition'): role_map[tid][p['teamPosition']] = p['participantId']

    rows = []
    for p in participants_info:
        pid = p['participantId']
        tid = p['teamId']
        pos = p['teamPosition']
        if not pos: continue

        enemy_team = 200 if tid == 100 else 100
        enemy_pid = role_map[enemy_team].get(pos)
        
        enemy_champ = "None"
        if enemy_pid:
            enemy_data = next((x for x in participants_info if x['participantId'] == enemy_pid), None)
            if enemy_data: enemy_champ = enemy_data['championName']

        # --- CORREÇÃO DO NOME AQUI ---
        # Monta o Riot ID corretamente: Nome + # + TAG
        game_name = p.get('riotIdGameName')
        tag_line = p.get('riotIdTagline')
        
        if game_name and tag_line:
            full_name = f"{game_name}#{tag_line}"
        else:
            full_name = p.get('summonerName', 'Desconhecido') # Fallback
            
        stats = {
            'Qtd_Partidas': 1, 'Match ID': match_id, 'Patch': patch,
            'Champion': p['championName'], 'Enemy Champion': enemy_champ,
            'Game Start Time': start_time, 'Win Rate %': 1 if p['win'] else 0,
            
            # AGORA SALVA O NOME CERTO:
            'Player Name': full_name, 
            'PUUID': p['puuid'],
            
            # KDA & Combate
            'Kills': p.get('kills', 0), 'Deaths': p.get('deaths', 0), 'Assists': p.get('assists', 0),
            'KDA': safe_div(p.get('kills', 0) + p.get('assists', 0), p.get('deaths', 1)),
            'Kill Participation': safe_div(p.get('kills', 0) + p.get('assists', 0), info['teams'][0]['objectives']['champion']['kills'] if tid==100 else info['teams'][1]['objectives']['champion']['kills']),
            'Total Damage Dealt': p.get('totalDamageDealtToChampions', 0),
            'Total Damage Taken': p.get('totalDamageTaken', 0),
            'Self Mitigated Damage': p.get('damageSelfMitigated', 0),
            
            # Economia
            'Gold Earned': p.get('goldEarned', 0),
            'Farm/Min': safe_div(p.get('totalMinionsKilled', 0) + p.get('neutralMinionsKilled', 0), duration_min),
            'Damage/Min': safe_div(p.get('totalDamageDealtToChampions', 0), duration_min),
            'Gold/Min': safe_div(p.get('goldEarned', 0), duration_min),
            
            # Visão e Objetivos
            'Vision Score': p.get('visionScore', 0),
            'Vision Score/Min': safe_div(p.get('visionScore', 0), duration_min),
            'Wards Placed': p.get('wardsPlaced', 0),
            'Wards Killed': p.get('wardsKilled', 0),
            'Control Wards Placed': p.get('detectorWardsPlaced', 0),
            'Damage to Buildings': p.get('damageDealtToBuildings', 0),
            'Damage to Objectives': p.get('damageDealtToObjectives', 0),
            'Turret Plates Taken': p.get('turretPlatesTaken', 0),
            
            # Extras
            'Team Damage %': safe_div(p.get('totalDamageDealtToChampions', 0), team_totals[tid]['dmg']),
            'Damage Taken %': safe_div(p.get('totalDamageTaken', 0), team_totals[tid]['taken']),
            'First Blood Kill': 1 if p.get('firstBloodKill') else 0,
            'First Blood Assist': 1 if p.get('firstBloodAssist') else 0,
            'First Tower Kill': 1 if p.get('firstTowerKill') else 0,
            'First Tower Assist': 1 if p.get('firstTowerAssist') else 0,
            'CC Score': p.get('timeCCingOthers', 0)
        }
        
        minutes_to_check = [5, 6, 11, 12, 14, 18, 20]
        for t in minutes_to_check:
            my_snap = get_snapshot_at_minute(frames, t, pid, tid, participants_info)
            en_snap = get_snapshot_at_minute(frames, t, enemy_pid, enemy_team, participants_info) if enemy_pid else None
            my_events = get_events_at_minute(timeline, t, pid)
            
            if my_snap:
                suffix = f"{t}'"
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
                    stats[f"Gold Eff {suffix}"] = safe_div(my_snap['damage'], my_snap['gold'])
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

                if t == 6: stats["CS aos 6 min"] = my_snap['cs']
                if t == 12:
                    stats["CS aos 12 min"] = my_snap['cs']
                    stats["Gold aos 12 min"] = my_snap['gold']
                    stats["XP aos 12 min"] = my_snap['xp']
                    stats["Deaths até 12min"] = my_events['deaths']
                    stats["VPM @12"] = safe_div(p['visionScore'], 12)
                    stats["KDA @12"] = safe_div(my_events['kills'] + my_events['assists'], my_events['deaths'])
                if t == 18: stats["CS aos 18 min"] = my_snap['cs']

        rows.append(stats)
    return rows

def get_puuids_from_names():
    player_data = []
    print("🔍 Buscando PUUIDs dos jogadores...")
    for riot_id in ALVOS:
        try:
            if '#' not in riot_id: continue
            name, tag = riot_id.split('#')
            account = riot_watcher.account.by_riot_id(REGION_ACCOUNT, name, tag)
            player_data.append({'riot_id': riot_id, 'puuid': account['puuid']})
            print(f" > Encontrado: {riot_id}")
        except Exception as e:
            print(f"❌ Erro ao buscar {riot_id}: {e}")
        time.sleep(1)
    return player_data

def load_processed_ids():
    processed = set()
    try:
        # Verifica se a tabela existe antes de tentar ler
        insp = inspect(engine)
        if not insp.has_table("partidas_br"):
            print("⚠️ Tabela 'partidas_br' não existe. Será criada.")
            return processed

        with engine.connect() as conn:
            query = text('SELECT DISTINCT "Match ID" FROM partidas_br')
            df_db = pd.read_sql(query, conn)
            # Garante que é string para comparar direito
            processed.update(df_db['Match ID'].astype(str))
            print(f"✅ Histórico carregado: {len(processed)} partidas já processadas.")
            
    except Exception as e:
        print(f"❌ ERRO CRÍTICO ao ler banco de dados: {e}")
        # Se não conseguir ler o histórico, é melhor PARAR do que duplicar tudo
        sys.exit(1) 
        
    return processed

def main():
    players = get_puuids_from_names()
    if not players: return

    processed_ids = load_processed_ids()
    new_match_ids = set()
    
    print("\n🔍 Buscando partidas recentes...")
    for p in players:
        try:
            # Mantivemos count=10 para garantir o histórico
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
        
        time.sleep(2.5) 
        print(f" [{i+1}/{len(match_list)}] Processado...")

    if buffer:
        df_new = pd.DataFrame(buffer)
        print("💾 Salvando na tabela 'partidas_br'...")
        try:
            df_new.to_sql('partidas_br', engine, if_exists='append', index=False, chunksize=500)
            print("✅ SUCESSO! Dados salvos com Nomes Corrigidos.")
        except Exception as e:
            print(f"❌ Erro de Banco: {e}")

if __name__ == "__main__":
    main()
