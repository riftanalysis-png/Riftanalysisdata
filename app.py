import pandas as pd
import time
import os
import random
import sys
import json
from datetime import datetime
import glob
from riotwatcher import LolWatcher, ApiError
from sqlalchemy import create_engine, text

# --- CONFIGURAÇÃO ---
API_KEY = os.environ.get("RIOT_API_KEY") 
DB_URL = os.environ.get("DB_URL") # URL do Supabase
REGION = 'kr'
MATCH_TARGET = 3

# Configuração de Backup CSV (Data Lake)
DATA_FOLDER = 'dados'
TODAY_STR = datetime.now().strftime('%Y-%m-%d')
FILE_TODAY = f'{DATA_FOLDER}/{TODAY_STR}.csv'

sys.stdout.reconfigure(line_buffering=True)

if not API_KEY:
    print("ERRO: RIOT_API_KEY ausente.")
    sys.exit(1)
if not DB_URL:
    print("ERRO: DB_URL (Supabase) ausente.")
    sys.exit(1)

watcher = LolWatcher(API_KEY)
# Conexão com o Banco de Dados
engine = create_engine(DB_URL)

# --- FUNÇÕES AUXILIARES ---
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
        match = watcher.match.by_id(REGION, match_id)
        timeline = watcher.match.timeline_by_match(REGION, match_id)
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
            
            'Kills': p['kills'], 'Deaths': p['deaths'], 'Assists': p['assists'],
            'KDA': safe_div(p['kills'] + p['assists'], p['deaths']),
            'Kill Participation': safe_div(p['kills'] + p['assists'], team_totals[tid]['kills']),
            'Total Damage Dealt': p['totalDamageDealtToChampions'],
            'Total Damage Taken': p['totalDamageTaken'],
            'Self Mitigated Damage': p['damageSelfMitigated'],
            
            'Gold Earned': p['goldEarned'],
            'Farm/Min': safe_div(p['totalMinionsKilled'] + p['neutralMinionsKilled'], duration_min),
            'Damage/Min': safe_div(p['totalDamageDealtToChampions'], duration_min),
            'Gold/Min': safe_div(p['goldEarned'], duration_min),
            
            'Vision Score': p['visionScore'],
            'Vision Score/Min': safe_div(p['visionScore'], duration_min),
            'Wards Placed': p['wardsPlaced'], 'Wards Killed': p['wardsKilled'],
            'Control Wards Placed': p['detectorWardsPlaced'],
            
            'Damage to Buildings': p['damageDealtToBuildings'],
            'Damage to Objectives': p['damageDealtToObjectives'],
            'Turret Plates Taken': p.get('turretPlatesTaken', 0),
            
            'Team Damage %': safe_div(p['totalDamageDealtToChampions'], team_totals[tid]['dmg']),
            'Damage Taken %': safe_div(p['totalDamageTaken'], team_totals[tid]['taken']),
            
            'First Blood Kill': 1 if p.get('firstBloodKill') else 0,
            'First Blood Assist': 1 if p.get('firstBloodAssist') else 0,
            'First Tower Kill': 1 if p.get('firstTowerKill') else 0,
            'First Tower Assist': 1 if p.get('firstTowerAssist') else 0,
            'CC Score': p['timeCCingOthers']
        }
        
        target_minutes = [5, 6, 11, 12, 14, 18, 20]
        for t in target_minutes:
            my_cs, my_gold, my_xp, my_lvl = get_stats_at_minute(frames, t, pid)
            my_k, my_d, my_a, my_plates = get_event_stats_at_minute(timeline['info'], t, pid)
            en_cs, en_gold, en_xp, en_lvl = get_stats_at_minute(frames, t, enemy_pid)
            team_gold_at_t = get_team_total_at_minute(frames, t, tid, info['participants'])
            
            my_dmg_est = round((p['totalDamageDealtToChampions'] / duration_min) * t, 2)
            en_dmg_est = round((enemy_data['totalDamageDealtToChampions'] / duration_min) * t, 2)
            suffix = f"{t}'"
            
            if t in [5, 11, 12, 14, 20]:
                stats[f'Kills {suffix}'] = my_k; stats[f'Deaths {suffix}'] = my_d; stats[f'Assists {suffix}'] = my_a
                stats[f'CS {suffix}'] = my_cs; stats[f'Gold Earned {suffix}'] = my_gold; stats[f'Plates {suffix}'] = my_plates
                stats[f'KDA {suffix}'] = safe_div(my_k + my_a, my_d)
                stats[f'GPM {suffix}'] = safe_div(my_gold, t); stats[f'DPM {suffix}'] = safe_div(my_dmg_est, t)
                stats[f'Gold Share {suffix}'] = safe_div(my_gold, team_gold_at_t)
                stats[f'Gold Eff {suffix}'] = safe_div(my_dmg_est, my_gold)
                stats[f'CS Diff {suffix}'] = my_cs - en_cs; stats[f'Gold Diff {suffix}'] = my_gold - en_gold
                stats[f'XP Diff {suffix}'] = my_xp - en_xp; stats[f'DMG Diff {suffix}'] = round(my_dmg_est - en_dmg_est, 2)
            
            if t == 12:
                stats['CS aos 12 min'] = my_cs; stats['Gold aos 12 min'] = my_gold; stats['XP aos 12 min'] = my_xp
                stats['Deaths até 12min'] = my_d; stats['VPM @12'] = round(safe_div(p['visionScore'], duration_min) * 12, 2)
                stats['KDA @12'] = safe_div(my_k + my_a, my_d)
            if t == 6: stats['CS aos 6 min'] = my_cs
            if t == 18: stats['CS aos 18 min'] = my_cs

        rows.append(stats)
    return rows

def load_processed_ids_from_db():
    # Consulta o Banco de Dados para saber quais partidas já temos
    processed = set()
    try:
        # Tenta ler apenas a coluna 'Match ID' da tabela 'partidas'
        with engine.connect() as conn:
            # Verifica se a tabela existe primeiro
            # O jeito mais simples em Pandas é tentar um select limit 1
            query = text('SELECT "Match ID" FROM partidas')
            df_db = pd.read_sql(query, conn)
            processed.update(df_db['Match ID'].astype(str))
            print(f"Histórico no Banco de Dados: {len(processed)} partidas.")
    except Exception as e:
        print("Tabela 'partidas' ainda não existe ou erro na conexão (normal no 1º uso).")
        print(f"Detalhe: {e}")
    return processed

def collect_match_ids(target_amount, processed_ids):
    all_match_ids = set()
    print("Conectando à Liga Master...")
    try:
        entries = watcher.league.masters_by_queue(REGION, 'RANKED_SOLO_5x5')['entries']
        random.shuffle(entries)
        for i, entry in enumerate(entries):
            if len(all_match_ids) >= target_amount: break
            try:
                puuid = entry.get('puuid')
                if not puuid:
                    summ_id = entry.get('summonerId')
                    if summ_id: puuid = watcher.summoner.by_id(REGION, summ_id)['puuid']
                    else: continue
                
                # Baixa 20 partidas do jogador
                matches = watcher.match.matchlist_by_puuid(REGION, puuid, count=20)
                # Filtra o que JÁ TEMOS no banco
                new_matches = [m for m in matches if str(m) not in processed_ids]
                all_match_ids.update(new_matches)
                print(f" > Jogador OK. Novas na fila: {len(all_match_ids)}")
                time.sleep(1.0)
            except: continue
    except: pass
    return list(all_match_ids)[:target_amount]

def upload_to_db(df_novo):
    if df_novo.empty: return
    print("Iniciando upload para o Supabase (Postgres)...")
    try:
        # A Mágica do Pandas: if_exists='append' cria a tabela se não existir
        # chunksize ajuda a não sobrecarregar a conexão
        df_novo.to_sql('partidas', engine, if_exists='append', index=False, chunksize=500)
        print("SUCESSO: Dados salvos no Banco de Dados!")
    except Exception as e:
        print(f"ERRO ao salvar no Banco: {e}")

def main():
    # 1. Consulta o Banco para não repetir trabalho
    processed_ids = load_processed_ids_from_db()
    
    # 2. Coleta novos IDs
    match_ids = collect_match_ids(MATCH_TARGET, processed_ids)
    
    if not match_ids:
        print("Sem partidas novas.")
        return

    print(f"Processando {len(match_ids)} partidas...")
    buffer = []
    for i, m_id in enumerate(match_ids):
        data = process_match(m_id)
        if data: buffer.extend(data)
        time.sleep(1.2)

    if buffer:
        df_new = pd.DataFrame(buffer)
        
        # 3. Salva no Supabase (Fonte da Verdade)
        upload_to_db(df_new)
        
        # 4. Salva Backup CSV no GitHub (Data Lake Diário - Opcional mas recomendado)
        if not os.path.exists(DATA_FOLDER): os.makedirs(DATA_FOLDER)
        header = not os.path.isfile(FILE_TODAY)
        # Salva CSV com formato BR para leitura fácil humana, se precisar
        df_new.to_csv(FILE_TODAY, mode='a', index=False, sep=';', decimal=',', header=header)
        print(f"Backup CSV salvo em: {FILE_TODAY}")

if __name__ == "__main__":
    main()
