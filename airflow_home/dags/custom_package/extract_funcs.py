
import requests
import numpy as np
import pandas as pd 
import urllib.request
import time
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import json
import re
import time
import concurrent.futures
import itertools
import sys
import json
import multiprocessing
from riotwatcher import LolWatcher, ApiError

##### Extraction Functions:

##### Scraping

# get streamer data
def get_streamers(choice):
    '''
    :choice:
        If choice == online, then it will only scrape the data of those that are online at the moment.
        If choice == all. then it will scrape all player data, regardless of whether online or not.
    '''
    if choice == "online":
        sys.setrecursionlimit(100000)
        central_url = 'https://www.trackingthepros.com/d/list_players?'
        response = requests.get(central_url)
        temp_dict = json.loads(response.text) # combination of json and html
        ingame_df = pd.DataFrame(temp_dict['data'])
        ingame_df = ingame_df[(ingame_df['home'] == 'Europe') & # Applying constraint for this project 
                              (ingame_df['current'] == 'Europe') & # Applying constraint for this project
                              (ingame_df['roleNum'].isin([1,2,3,4,5]))] # filtering for the basic roles
        ingame_df['link'] = ingame_df['name'].apply(lambda x: x.split("'")[-2])
        columns_we_want = ['DT_RowId', 'plug', 'roleNum', 'role', 'rankHigh', 'onlineNum', 'link']
        return_df = ingame_df[ingame_df['onlineNum'] > 0][columns_we_want]
        # This ensures someone has started a game
        print('Number of streamers playing right now: ', len(return_df))
        return return_df    
    elif choice == "all": 
        sys.setrecursionlimit(100000)
        central_url = 'https://www.trackingthepros.com/d/list_players?'
        response = requests.get(central_url)
        temp_dict = json.loads(response.text) # combination of json and html
        ingame_df = pd.DataFrame(temp_dict['data'])
        ingame_df = ingame_df[(ingame_df['home'] == 'Europe') & # Applying constraint for this project 
                              (ingame_df['current'] == 'Europe') & # Applying constraint for this project
                              (ingame_df['roleNum'].isin([1,2,3,4,5]))] # filtering for the basic roles
        ingame_df['link'] = ingame_df['name'].apply(lambda x: x.split("'")[-2])
        columns_we_want = ['DT_RowId', 'plug', 'roleNum', 'role', 'rankHigh', 'onlineNum', 'link']
        return_df = ingame_df[columns_we_want]
        # This gets all the players
        print('Total number of players in EU server: ', len(return_df))
        return return_df

# getting player info
def player_details(df):
    links = df['link'].tolist()
    unique_ref = df.DT_RowId.tolist()
    frame_list = []
    for link in links:
        response = requests.get(link)
        soup = BeautifulSoup(response.text,'html.parser')
        table = soup.find('table')
        dataframe = pd.read_html(str(table))[0]
        dataframe.set_index(0, drop=True, inplace=True)
        frame_list.append(dataframe)
        frame2 = pd.concat(frame_list, axis=1)
        frame2 = frame2.T
    frame2['DT_RowId'] = unique_ref        
    return frame2

# getting last match deets which will be used to get more info from Riot API
def last_match_key(list_of_tpp_ids):
    '''
    tpp_ids: refers to list of ids given to each unique player on the streamer website, top pro players
    '''
    tpp_ids = list_of_tpp_ids
    l1 = []
    l2 = []
    l3 = []
    for tpp_id in tpp_ids:
        try: ###### CHANGES MADE
            tpp_id_games = 'https://www.trackingthepros.com/d/pro_feed?player_id={}&'.format(str(tpp_id))
            response = requests.get(tpp_id_games)
            temp_dict = json.loads(response.text)
            last_match_final = temp_dict['data'][0]['card_data']
            account_name = last_match_final['account_name']
            account_server = last_match_final['account_server']
            l1.append(account_name)
            l2.append(account_server)
            l3.append(tpp_id)
        except:
            continue
    frame = pd.DataFrame({'account_name':l1, 'region':l2, 'DT_RowId': l3})
    return frame

# To be used by a later function for multiprocessing
def split(a, n):
    k, m = divmod(len(a), n)
    return list(a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))

# multiprocessing/threading:
def multi_processing(df):
    tpp_ids = df['DT_RowId'].tolist()
    cpu_count=multiprocessing.cpu_count()    
    if cpu_count > 1:
        to_be_concat = []
        split_tpp_ids = split(tpp_ids, cpu_count)
        with concurrent.futures.ProcessPoolExecutor() as executor: 
            results = [executor.submit(last_match_key, tpp_id) for tpp_id in split_tpp_ids]
            for g in concurrent.futures.as_completed(results):
                try: 
                    to_be_concat.append(g.result())
                except:
                    continue
        concat_df = pd.concat(to_be_concat)
        return concat_df
    elif cpu_count == 1:
        to_be_concat = []
        split_tpp_ids = split(tpp_ids, 2)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor: 
            results = [executor.submit(last_match_key, tpp_id) for tpp_id in split_tpp_ids]
            for g in concurrent.futures.as_completed(results):
                to_be_concat.append(g.result())
        concat_df = pd.concat(to_be_concat)
        return concat_df     

##### Riot API Functions

# Get important ids of players (without which we can't extract information)
def get_puuid(df):
    '''  
    Requires df with last match of players online. 
    Retrives an important unique ID, called puuid, that allows us to access data we can augment with from API. 
    Very time intensive.   
    '''
    lol_watcher = LolWatcher('RGAPI-xxxxxxxxxxx') # API Key     
    summonerName_list = df.account_name.tolist()    
    summoner_info_list = []
    for summonerName in summonerName_list:
        try: 
            summoner_info = lol_watcher.summoner.by_name('EUW1', summonerName)
            summoner_info_row = {}
            for key, value in summoner_info.items():
                summoner_info_row[key] = value
            summoner_info_list.append(summoner_info_row)
            time.sleep(0.2)
        except Exception:
            continue
            time.sleep(5)
    return_df = pd.DataFrame(summoner_info_list)
    return_df['account_name'] = summonerName_list
    return return_df

# Get match statistics
def match_statistics(df, number_of_matches): #### CHANGES MADE: CAN CHOOSE NUMBER OF MATCHES PER PLAYER FROM HISTORY
    '''    
    Generates statistics of player's last game. 
    The Riot API endpoint can only take puuid as arguments. 
    Returns two dataframes
    '''   
    lol_watcher = LolWatcher('RGAPI-2ba156f9-d150-4876-a51d-1595eaae9a6b') # API Key 
    
    list_of_puuids = df.puuid.tolist()
    puuids_of_interest = list_of_puuids
    wards_placed = []
    all_kills = []
    scores_at_10 = []
    match_info = []
    player_match_info = []
    chosen_champion = []
    
    for puuid in puuids_of_interest: 
        time.sleep(4)
        
        ########## Timeline Information
        try:
            matchid = lol_watcher.match.matchlist_by_puuid('Europe', puuid, queue = 420, count=number_of_matches)[0]
            temp1 = lol_watcher.match.timeline_by_match('europe', matchid)
        except:
            continue
            time.sleep(10)
        match_id = temp1['metadata']['matchId']
        temp2 = temp1["info"]["frames"]
        puuid_pos = temp1['metadata']['participants'].index(puuid) + 1
        
        #### Duration - if duration is less than 10 minutes, then there was an error in matchmaking, someone quit, trolled, etc. 
        duration = temp2[-1]['timestamp']
        if duration < 600000:
            continue
        else:
            match_information = {}
            match_information['match_id'] = match_id
            match_information['duration'] = duration
            match_info.append(match_information)

        #### Wards and vision
        for frame1 in temp2:
            for event1 in frame1["events"]:
                wards_row = {}
                if "wardType"in event1.keys() and "creatorId" in event1.keys() and event1["creatorId"] == puuid_pos:
                    wards_row["match_id"] = match_id
                    wards_row["puuid"] = puuid
                    wards_row["participantId"] = event1["creatorId"]
                    wards_row["wardIncident"] = event1["type"]
                    wards_row["wardType"] = event1["wardType"]
                    wards_row["timestamp"] = round(event1["timestamp"])
                elif "wardType"in event1.keys() and "killerId" in event1.keys() and event1["killerId"] == puuid_pos:
                    wards_row["match_id"] = match_id
                    wards_row["puuid"] = puuid
                    wards_row["participantId"] = event1["killerId"]
                    wards_row["wardIncident"] = event1["type"]
                    wards_row["wardType"] = event1["wardType"]
                    wards_row["timestamp"] = round(event1["timestamp"])
                wards_placed.append(wards_row)

        #### Deaths and Positions
        for frame2 in temp2:
            for event2 in frame2["events"]:
                kills_row = {}
                if "killerId"in event2.keys() and "victimId" in event2.keys():
                    if (event2["killerId"] == puuid_pos) or (event2["victimId"] == puuid_pos):
                        kills_row["match_id"] = match_id
                        kills_row["puuid"] = puuid
                        kills_row["main"] = puuid_pos
                        kills_row["killerId"] = event2["killerId"]
                        kills_row["victimId"] = event2["victimId"]
                        kills_row["timestamp"] = event2["timestamp"]
                all_kills.append(kills_row)
        
        #### Minions, Gold, XP
        for frame3 in temp2:
            participants_row = {}
            if frame3['timestamp'] >= 600000: 
                participant_specifics = frame3['participantFrames'][str(puuid_pos)]
                participants_row['match_id'] = match_id
                participants_row["puuid"] = puuid
                participants_row['gold_at_10'] = participant_specifics['totalGold']
                participants_row['minions_at_10'] = participant_specifics['minionsKilled']
                participants_row['xp_at_10'] = participant_specifics['xp']
                scores_at_10.append(participants_row)
                break
        
        #### Winner
        winningTeam = temp2[-1]['events'][-1]['winningTeam']
        player_match_information = {}
        player_match_information['match_id'] = match_id
        player_match_information['puuid'] = puuid
        if (winningTeam == 100) and (puuid_pos <= 5):
            player_match_information['outcome'] = 'win'
        elif (winningTeam == 200) and (puuid_pos <= 5):
            player_match_information['outcome'] = 'loss'
        elif (winningTeam == 200) and (puuid_pos >= 5):
            player_match_information['outcome'] = 'win'            
        else:
            player_match_information['outcome'] = 'loss' 
        player_match_info.append(player_match_information)
        
        temp3 = lol_watcher.match.by_id('Europe', match_id)['info'] # Calling on another API endpoint (rate limit applies)
        temp_pt = temp3['participants']
        for i in temp_pt:    
            if i['puuid'] == puuid:
                chosen_champ = {}
                chosen_champ['match_id'] = match_id
                chosen_champ['puuid'] = i['puuid']
                chosen_champ['champion_id'] = i['championId']
                chosen_champion.append(chosen_champ)
                break        

    #### Finishing off    
    ward_df = pd.DataFrame(wards_placed).dropna()
    kills_df = pd.DataFrame(all_kills).dropna()
    scores10_df = pd.DataFrame(scores_at_10).dropna()
    match_info_df = pd.DataFrame(match_info).dropna()
    player_match_df = pd.DataFrame(player_match_info).dropna()
    chosen_champion_df = pd.DataFrame(chosen_champion).dropna()
        
    
    return ward_df, kills_df, scores10_df, match_info_df, player_match_df, chosen_champion_df

# Getting champions data
def champions_dataframe():    
    '''    
    Helps extract champions from static champions API. 
    ___
    Note: Champions refer to the 140 playable characters in the game, that a player can select for a given match.
    '''    
    lol_watcher1 = LolWatcher('RGAPI-2ba156f9-d150-4876-a51d-1595eaae9a6b')
    
    version = lol_watcher1.data_dragon.versions_for_region('euw1')
    champions_version = version['n']['champion']
    current_champion_info = lol_watcher1.data_dragon.champions(champions_version)
    
    champions = []
    for component in current_champion_info["data"].values():
        champions_row = {}
        champions_row['champion_id'] = component['key']
        champions_row['champion_name'] = component['name']
        champions_row['attack'] = component['info']['attack']
        champions_row['defense'] = component['info']['defense']
        champions_row['magic'] = component['info']['magic']
        champions.append(champions_row)

    df = pd.DataFrame(champions)
    return df
