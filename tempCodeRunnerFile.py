data = pd.read_csv("/Users/nidhishgupta/Desktop/Dream11_Fantasy Team_Predictor/data/processed/full_req_data_final_final.csv")
# historical_data = pd.read_csv("/Users/nidhishgupta/Desktop/Dream11_Fantasy Team_Predictor/data/processed/historical_data_final_final.csv")
# model = joblib.load("/Users/nidhishgupta/Desktop/Dream11_Fantasy Team_Predictor/models/point_predicter_final.pkl")
# API_KEY = "cb8a5495-0125-4122-9cef-e0993d41c40f"
# SERIES_ID = "87c62aac-bc3c-4738-ab93-19da0690488f"
# url = f"https://api.cricapi.com/v1/series_squad?apikey={API_KEY}&id={SERIES_ID}"
# response = requests.get(url)
# data_api_1 = response.json()
# def get_next_match(series_info_json):
#     match_list = series_info_json['data']['matchList']
    
#     now = datetime.now(timezone.utc)
#     upcoming_matches = []
    
#     for match in match_list:
#         # Make match_time timezone-aware (UTC)
#         match_time = datetime.fromisoformat(match['dateTimeGMT']).replace(tzinfo=timezone.utc)
        
#         if match_time > now and not match['matchStarted']:
#             upcoming_matches.append(match)
    
#     if not upcoming_matches:
#         return None
    
#     # Sort by actual datetime (better than string sort)
#     upcoming_matches.sort(
#         key=lambda x: datetime.fromisoformat(x['dateTimeGMT']).replace(tzinfo=timezone.utc)
#     )
    
#     next_match = upcoming_matches[0]
    
#     return {
#         "team1": next_match['teams'][0],
#         "team2": next_match['teams'][1],
#         "venue": next_match['venue'],
#         "date": next_match['dateTimeGMT']
#     }
# def get_team_squads(data, team1, team2):
#     team1_squad = []
#     team2_squad = []

#     for team in data['data']:
#         team_name = team['teamName']

#         if team_name.lower() == team1.lower():
#             team1_squad = [player['name'] for player in team['players']]

#         elif team_name.lower() == team2.lower():
#             team2_squad = [player['name'] for player in team['players']]

#     return team1_squad, team2_squad
# url = f"https://api.cricapi.com/v1/series_info?apikey={API_KEY}&id={SERIES_ID}"
# response = requests.get(url)
# data_api_2 = response.json()
# next_match = get_next_match(data_api_2)
# print(next_match)
# team1 = next_match['team1']
# team2 = next_match['team2']
# venue = next_match['venue']
# print(venue)
# team1_squad,team2_squad = get_team_squads(data_api_1,team1,team2)
# print("Team 1 Squad:", team1_squad)
# print("Team 2 Squad:", team2_squad)
# squad = team1_squad+team2_squad
# session = SessionLocal()
# unique_players = session.query(
#     PlayerMatchStats.player_name
# ).distinct().all()
# session.close()
# unique_players = [p[0] for p in unique_players]
# def map_player_name(name, unique_players):
#     match = get_close_matches(name, unique_players, n=1, cutoff=0.85)
#     return match[0] if match else None


# mapped_players_1 = [
#     m for p in team1_squad 
#     if (m := map_player_name(p, unique_players)) is not None
# ]

# mapped_players_2 = [
#     m for p in team2_squad 
#     if (m := map_player_name(p, unique_players)) is not None
# ]
# all_mapped = mapped_players_1 + mapped_players_2