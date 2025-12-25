import pandas as pd
import time
import os
import random
import sys
import json
from riotwatcher import LolWatcher, ApiError
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe

# --- CONFIGURAÇÃO ---
API_KEY = os.environ.get("RIOT_API_KEY") 
GCP_SA_KEY = os.environ.get("GCP_SA_KEY") 
SHEET_ID = os.environ.get("SHEET_ID")     
REGION = 'kr'
MATCH_TARGET = 3 
FILE_RAW = 'Historico_Bruto_Completo.csv'

sys.stdout.reconfigure(line_buffering=True)

if not API_KEY:
    print("ERRO: RIOT_API_KEY ausente.")
    sys.exit(1)

watcher = LolWatcher(API_KEY)

# --- FUNÇÕES AUXILIARES ---
def get_clean_version(version_str):
    parts = version_str.split('.')
    return f"{parts[0]}.{parts[1]}" if len(parts) >= 2 else version_str

def safe_div(a, b):
    return a / b if b != 0 else 0

def get_stats_at_minute(frames, minute, pid):
    # Retorna: CS, Gold, XP, Level
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
    # Filtra IDs do time
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
                # Verifica se o jogador pegou a barricada (killer ou assist)
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
    duration_sec = info['gameDuration']
    duration_min = duration_sec / 60
    if duration_min < 15: return [] 
    
    patch = get_clean_version(info['gameVersion'])
    frames = timeline['info']['frames']
    start_time = info['gameCreation'] # Timestamp

    # Mapeamento de Rotas e Totais do Time
    role_map = {100: {}, 200: {}}
    p_info_dict = {}
    team_totals = {
        100: {'kills': 0, 'dmg': 0, 'taken': 0, 'gold': 0}, 
        200: {'kills': 0, 'dmg': 0, 'taken': 0, 'gold': 0}
    }

    # Pré-processamento para totais
    for p in info['participants']:
        tid = p['teamId']
        team_totals[tid]['kills'] += p['kills']
        team_totals[tid]['dmg'] += p['totalDamageDealtToChampions']
        team_totals[tid]['taken'] += p['totalDamageTaken']
        team_totals[tid]['gold'] += p['goldEarned']
        
        p_info_dict[p['participantId']] = p
        if p.get('teamPosition'): 
            role_map[tid][p['teamPosition']] = p['participantId']

    rows = []
    
    # LOOP PRINCIPAL DOS JOGADORES
    for p in info['participants']:
        pid = p['participantId']
        tid = p['teamId']
        pos = p['teamPosition']
        
        # Ignora se não tiver posição definida (ex: Arena/ARAM bugado)
        if not pos: continue

        enemy_team = 200 if tid == 100 else 100
        enemy_pid = role_map[enemy_team].get(pos)
        enemy_data = p_info_dict.get(enemy_pid) if enemy_pid else None
        
        if not enemy_data: continue

        # --- DADOS GERAIS (FINAL DO JOGO) ---
        stats = {
            'Qtd_Partidas': 1,
            'Match ID': match_id,
            'Patch': patch,
            'Champion': p['championName'],
            'Enemy Champion': enemy_data['championName'],
            'Game Start Time': start_time,
            'Win Rate %': 1 if p['win'] else 0, # Para média na Pivot
            
            # KDA & Combat
            'Kills': p['kills'],
            'Deaths': p['deaths'],
            'Assists': p['assists'],
            'KDA': safe_div(p['kills'] + p['assists'], p['deaths']),
            'Kill Participation': safe_div(p['kills'] + p['assists'], team_totals[tid]['kills']),
            'Total Damage Dealt': p['totalDamageDealtToChampions'],
            'Total Damage Taken': p['totalDamageTaken'],
            'Self Mitigated Damage': p['damageSelfMitigated'],
            
            # Economy & Farming
            'Gold Earned': p['goldEarned'],
            'Farm/Min': safe_div(p['totalMinionsKilled'] + p['neutralMinionsKilled'], duration_min),
            'Damage/Min': safe_div(p['totalDamageDealtToChampions'], duration_min),
            'Gold/Min': safe_div(p['goldEarned'], duration_min),
            
            # Vision
            'Vision Score': p['visionScore'],
            'Vision Score/Min': safe_div(p['visionScore'], duration_min),
            'Wards Placed': p['wardsPlaced'],
            'Wards Killed': p['wardsKilled'],
            'Control Wards Placed': p['detectorWardsPlaced'],
            
            # Objectives & Structures
            'Damage to Buildings': p['damageDealtToBuildings'],
            'Damage to Objectives': p['damageDealtToObjectives'],
            'Turret Plates Taken': p.get('turretPlatesTaken', 0), # Nem sempre a API traz direto aqui
            
            # Team Contribution %
            'Team Damage %': safe_div(p['totalDamageDealtToChampions'], team_totals[tid]['dmg']),
            'Damage Taken %': safe_div(p['totalDamageTaken'], team_totals[tid]['taken']),
            
            # First Events
            'First Blood Kill': 1 if p.get('firstBloodKill') else 0,
            'First Blood Assist': 1 if p.get('firstBloodAssist') else 0,
            'First Tower Kill': 1 if p.get('firstTowerKill') else 0,
            'First Tower Assist': 1 if p.get('firstTowerAssist') else 0,
            
            # CC
            'CC Score': p['timeCCingOthers']
        }
        
        # --- DADOS TEMPORAIS (5, 11, 12, 14, 20) ---
        # Lista de minutos solicitados + extras (6 e 18)
        target_minutes = [5, 6, 11, 12, 14, 18, 20]
        
        for t in target_minutes:
            # Stats Meus
            my_cs, my_gold, my_xp, my_lvl = get_stats_at_minute(frames, t, pid)
            my_k, my_d, my_a, my_plates = get_event_stats_at_minute(timeline['info'], t, pid)
            
            # Stats Inimigo
            en_cs, en_gold, en_xp, en_lvl = get_stats_at_minute(frames, t, enemy_pid)
            
            # Totais do Time no minuto (para Gold Share)
            team_gold_at_t = get_team_total_at_minute(frames, t, tid, info['participants'])
            
            # Estimativas (Dano é linear pois a API não dá dano exato por minuto sem heavy processing)
            my_dmg_est = (p['totalDamageDealtToChampions'] / duration_min) * t
            en_dmg_est = (enemy_data['totalDamageDealtToChampions'] / duration_min) * t
            
            # Preenchendo colunas específicas pedidas
            suffix = f"{t}'" # Ex: 5'
            
            # Colunas Padrão para 5, 11, 12, 14, 20
            if t in [5, 11, 12, 14, 20]:
                stats[f'Kills {suffix}'] = my_k
                stats[f'Deaths {suffix}'] = my_d
                stats[f'Assists {suffix}'] = my_a
                stats[f'CS {suffix}'] = my_cs
                stats[f'Gold Earned {suffix}'] = my_gold
                stats[f'Plates {suffix}'] = my_plates
                stats[f'KDA {suffix}'] = safe_div(my_k + my_a, my_d)
                stats[f'GPM {suffix}'] = safe_div(my_gold, t)
                stats[f'DPM {suffix}'] = safe_div(my_dmg_est, t)
                stats[f'Gold Share {suffix}'] = safe_div(my_gold, team_gold_at_t)
                stats[f'Gold Eff {suffix}'] = safe_div(my_dmg_est, my_gold) # Eficiência = Dano gerado por Ouro gasto
                
                # Diferenciais (Diffs)
                stats[f'CS Diff {suffix}'] = my_cs - en_cs
                stats[f'Gold Diff {suffix}'] = my_gold - en_gold
                stats[f'XP Diff {suffix}'] = my_xp - en_xp
                stats[f'DMG Diff {suffix}'] = my_dmg_est - en_dmg_est
            
            # Colunas Específicas Solicitadas (Nomes exatos)
            if t == 12:
                stats['CS aos 12 min'] = my_cs
                stats['Gold aos 12 min'] = my_gold
                stats['XP aos 12 min'] = my_xp
                stats['Deaths até 12min'] = my_d
                stats['VPM @12'] = safe_div(p['visionScore'], duration_min) * 12 # Estimativa linear
                stats['KDA @12'] = safe_div(my_k + my_a, my_d)
                
            if t == 6:
                stats['CS aos 6 min'] = my_cs
                
            if t == 18:
                stats['CS aos 18 min'] = my_cs

        rows.append(stats)
    return rows

def collect_match_ids(target_amount):
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
                    if summ_id:
                        puuid = watcher.summoner.by_id(REGION, summ_id)['puuid']
                    else: continue

                matches = watcher.match.matchlist_by_puuid(REGION, puuid, count=10)
                all_match_ids.update(matches)
                print(f" > Jogador OK. Partidas: {len(all_match_ids)}")
                time.sleep(1.0)
            except: continue
    except Exception as e:
        print(f"Erro na liga: {e}")
    return list(all_match_ids)[:target_amount]

def upload_to_sheets(df_novo):
    if not GCP_SA_KEY or not SHEET_ID: return
    try:
        creds_dict = json.loads(GCP_SA_KEY)
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID)
        try: worksheet = sh.worksheet("DadosBrutos")
        except: worksheet = sh.sheet1
        
        existing_data = worksheet.get_all_values()
        if not existing_data: set_with_dataframe(worksheet, df_novo)
        else: set_with_dataframe(worksheet, df_novo, row=len(existing_data)+1, include_header=False)
        print("Upload Sheets OK!")
    except Exception as e: print(f"Erro Sheets: {e}")

def main():
    processed_ids = set()
    if os.path.isfile(FILE_RAW):
        try:
            df = pd.read_csv(FILE_RAW, sep=',', decimal='.')
            if 'Match ID' in df.columns: processed_ids = set(df['Match ID'].astype(str))
        except: pass

    match_ids = collect_match_ids(MATCH_TARGET + 5)
    match_ids = [mid for mid in match_ids if str(mid) not in processed_ids][:MATCH_TARGET]
    
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
        header = not os.path.isfile(FILE_RAW)
        df_new.to_csv(FILE_RAW, mode='a', index=False, sep=',', decimal='.', header=header)
        upload_to_sheets(df_new)
        print("CSV salvo e Sheets atualizado.")

if __name__ == "__main__":
    main()
