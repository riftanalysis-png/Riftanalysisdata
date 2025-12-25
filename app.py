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
GCP_SA_KEY = os.environ.get("GCP_SA_KEY") # Chave do Google (JSON inteiro)
SHEET_ID = os.environ.get("SHEET_ID")     # ID da Planilha
REGION = 'kr'
MATCH_TARGET = 3  # Meta diária
FILE_RAW = 'Historico_Bruto_Completo.csv'

# Força o Python a imprimir o log na hora
sys.stdout.reconfigure(line_buffering=True)

if not API_KEY:
    print("ERRO CRÍTICO: RIOT_API_KEY não encontrada!")
    sys.exit(1)

watcher = LolWatcher(API_KEY)

# --- FUNÇÕES GOOGLE SHEETS ---
def upload_to_sheets(df_novo):
    if not GCP_SA_KEY or not SHEET_ID:
        print("AVISO: Chaves do Google não configuradas. Upload ignorado.")
        return

    print("Iniciando upload para o Google Sheets...")
    try:
        # Autenticação
        creds_dict = json.loads(GCP_SA_KEY)
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)

        sh = client.open_by_key(SHEET_ID)
        
        try:
            worksheet = sh.worksheet("DadosBrutos")
        except:
            worksheet = sh.sheet1

        existing_data = worksheet.get_all_values()
        
        if not existing_data:
            set_with_dataframe(worksheet, df_novo)
        else:
            next_row = len(existing_data) + 1
            set_with_dataframe(worksheet, df_novo, row=next_row, include_header=False)
            
        print("SUCESSO: Dados enviados para o Google Sheets!")
        
    except Exception as e:
        print(f"ERRO ao enviar para o Sheets: {e}")

# --- FUNÇÕES RIOT ---
def get_clean_version(version_str):
    parts = version_str.split('.')
    return f"{parts[0]}.{parts[1]}" if len(parts) >= 2 else version_str

def get_stats_at_minute(frames, minute, pid):
    if minute >= len(frames): return 0, 0, 0
    frame = frames[minute]['participantFrames']
    pid_key = str(pid)
    if pid_key in frame:
        p = frame[pid_key]
        return (p['minionsKilled'] + p['jungleMinionsKilled']), p['totalGold'], p['xp']
    return 0, 0, 0

def get_event_stats(timeline_info, minute_limit, pid):
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

    role_map = {100: {}, 200: {}}
    p_info_dict = {}

    for p in info['participants']:
        t_id = p['teamId']; pos = p['teamPosition']; pid = p['participantId']
        p_info_dict[pid] = p
        if pos: role_map[t_id][pos] = pid

    rows = []
    for p in info['participants']:
        pid = p['participantId']
        team_id = p['teamId']
        my_pos = p['teamPosition']
        
        enemy_team = 200 if team_id == 100 else 100
        enemy_pid = role_map[enemy_team].get(my_pos)
        enemy_data = p_info_dict.get(enemy_pid) if enemy_pid else None
        
        if not enemy_data: continue

        stats = {
            'Match ID': match_id,
            'Champion': p['championName'],
            'Enemy Champion': enemy_data['championName'],
            'Result': 'Win' if p['win'] else 'Loss',
            'Patch': patch,
            'Lane': my_pos
        }
        
        time_points = [5, 11, 12, 14, 20] 
        for t in time_points:
            my_cs, my_gold, my_xp = get_stats_at_minute(frames, t, pid)
            en_cs, en_gold, en_xp = get_stats_at_minute(frames, t, enemy_pid) if enemy_pid else (0,0,0)
            k, d, a, plates = get_event_stats(timeline['info'], t, pid)
            
            my_dmg_est = (p['totalDamageDealtToChampions'] / duration_min) * t
            en_dmg_est = (enemy_data['totalDamageDealtToChampions'] / duration_min) * t if enemy_data else 0
            
            suffix = f"{t}'"
            stats[f'CS {suffix}'] = my_cs
            stats[f'Gold Diff {suffix}'] = my_gold - en_gold
            stats[f'XP Diff {suffix}'] = my_xp - en_xp
            stats[f'DMG Diff {suffix}'] = my_dmg_est - en_dmg_est
            
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
                    else:
                        continue

                matches = watcher.match.matchlist_by_puuid(REGION, puuid, count=10)
                all_match_ids.update(matches)
                print(f" > Jogador OK. Partidas acumuladas: {len(all_match_ids)}")
                time.sleep(1.0)
                
            except Exception as e:
                print(f"ERRO ao processar jogador: {e}")
                continue
                
    except Exception as e:
        print(f"ERRO CRÍTICO NA LIGA: {e}")
        
    return list(all_match_ids)[:target_amount]

def main():
    processed_ids = set()
    # Verifica CSV com vírgula (novo padrão)
    if os.path.isfile(FILE_RAW):
        try:
            df = pd.read_csv(FILE_RAW, sep=',', decimal='.')
            if 'Match ID' in df.columns:
                processed_ids = set(df['Match ID'].astype(str))
                print(f"Base Local carregada: {len(processed_ids)} partidas.")
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
        
        # SALVA COM VÍRGULA E PONTO (Internacional)
        header = not os.path.isfile(FILE_RAW)
        df_new.to_csv(FILE_RAW, mode='a', index=False, sep=',', decimal='.', header=header)
        print("CSV Local Atualizado.")
        
        upload_to_sheets(df_new)

if __name__ == "__main__":
    main()
