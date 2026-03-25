from sqlalchemy import func, Integer
from db.models import Match, PlayerMatchStats
from difflib import get_close_matches

# ✅ WICKETKEEPER LIST (normalized)
WICKETKEEPERS = set([
    'ab de villiers', 'adam gilchrist', 'aditya tare', 'ambati rayudu',
    'brendon mccullum', 'c. m. gautam', 'dishant yagnik',
    'davy jacobs', 'gurkeerat singh', 'heinrich klaasen',
    'ishan kishan', 'jos buttler', 'jonny bairstow', 'jitesh sharma',
    'josh inglis', 'arun karthik', 'kumar sangakkara',
    'dinesh karthik', 'kl rahul', 'kedar jadhav', 'srikar bharat',
    'kamran akmal', 'mahesh rawat', 'manvinder bisla', 'ms dhoni',
    'matthew wade', 'mark boucher', 'nicholas pooran', 'navdeep saini',
    'naman ojha', 'prabhsimran singh', 'parthiv patel', 'pinal shah',
    'quinton de kock', 'ryan rickelton', 'rishabh pant',
    'robin uthappa', 'shreevats goswami', 'sheldon jackson',
    'sanju samson', 'sam billings', 'tristan stubbs', 'vishnu vinod',
    'wriddhiman saha', 'yogesh takawale'
])


# ✅ NEW: robust name mapping
def map_player_name(name, squad):
    match = get_close_matches(name, squad, n=1, cutoff=0.85)
    return match[0] if match else None


def ingest_match_data(api_response, session, team1_squad, team2_squad):
    data = api_response['data']

    team1_squad_norm = set([p.lower().strip() for p in team1_squad])
    team2_squad_norm = set([p.lower().strip() for p in team2_squad])

    combined_squad = team1_squad_norm.union(team2_squad_norm)  # ✅ ADDED for reuse

    team1, team2 = data['teams']
    venue = data['venue']

    # ✅ Generate numeric match_id
    max_id = session.query(
        func.max(func.cast(Match.match_id, Integer))
    ).scalar()

    match_id = str((max_id or 0) + 1)

    def get_pitch_type_for_venue(session, venue):
        result = session.query(Match.pitch_type)\
            .filter(Match.venue == venue)\
            .distinct()\
            .all()

        pitch_types = [r[0] for r in result if r[0] is not None]
        return pitch_types[0] if pitch_types else None

    mapped_pitch_type = get_pitch_type_for_venue(session, venue)

    match = Match(
        match_id=match_id,
        team1=team1,
        team2=team2,
        venue=venue,
        pitch_type=mapped_pitch_type
    )
    session.add(match)

    # ------------------------------
    # PROCESS SCORECARD
    # ------------------------------
    for inning_data in data['scorecard']:

        inning_name = inning_data['inning']
        batting_team = inning_name.split(" Inning")[0]
        bowling_team = team2 if batting_team == team1 else team1

        player_dict = {}
        lbw_bowled_tracker = {}

        # ----------------------
        # ✅ BATTING
        # ----------------------
        for i, batter in enumerate(inning_data['batting']):
            name_raw = batter['batsman']['name']
            name = name_raw.lower().strip()

            # ✅ CHANGED: apply mapping BEFORE using as key
            mapped_name = map_player_name(name, combined_squad) or name

            if mapped_name not in player_dict:
                player_dict[mapped_name] = {
                    'display_name': name_raw,
                    'runs': 0, 'balls': 0, 'fours': 0, 'sixes': 0,
                    'balls_bowled': 0, 'runs_conceded': 0, 'wickets': 0,
                    'fielding_points': 0, 'bat_pos': i + 1,
                    'maiden_bonus': 0
                }

            stats = player_dict[mapped_name]
            stats['runs'] += batter.get('r', 0)
            stats['balls'] += batter.get('b', 0)
            stats['fours'] += batter.get('4s', 0)
            stats['sixes'] += batter.get('6s', 0)

            # LBW/BOWLED tracking (WITH NAME MAPPING)
            dismissal_type = batter.get('dismissal', '').lower()
            if dismissal_type in ['lbw', 'bowled']:
                bowler_name_raw = batter.get('bowler', {}).get('name')
                if bowler_name_raw:
                    bowler_name = bowler_name_raw.lower().strip()

                    mapped_bowler = map_player_name(
                        bowler_name,
                        combined_squad
                    ) or bowler_name

                    lbw_bowled_tracker[mapped_bowler] = lbw_bowled_tracker.get(mapped_bowler, 0) + 1

        # ----------------------
        # ✅ BOWLING
        # ----------------------
        for bowler in inning_data['bowling']:
            name_raw = bowler['bowler']['name']
            name = name_raw.lower().strip()

            # ✅ CHANGED: apply mapping BEFORE using as key
            mapped_name = map_player_name(name, combined_squad) or name

            if mapped_name not in player_dict:
                player_dict[mapped_name] = {
                    'display_name': name_raw,
                    'runs': 0, 'balls': 0, 'fours': 0, 'sixes': 0,
                    'balls_bowled': 0, 'runs_conceded': 0, 'wickets': 0,
                    'fielding_points': 0, 'bat_pos': None,
                    'maiden_bonus': 0
                }

            stats = player_dict[mapped_name]

            overs = bowler.get('o', 0)
            whole, part = str(overs).split('.') if '.' in str(overs) else (overs, 0)
            balls = int(whole) * 6 + int(part)

            stats['balls_bowled'] += balls
            stats['runs_conceded'] += bowler.get('r', 0)
            stats['wickets'] += bowler.get('w', 0)

            # Maiden bonus
            maidens = bowler.get('m', 0)
            stats['maiden_bonus'] += maidens * 12

        # ----------------------
        # ✅ FIELDING
        # ----------------------
        for f in inning_data.get('catching', []):
            if 'catcher' not in f:
                continue

            name_raw = f['catcher']['name']
            name = name_raw.lower().strip()

            # ✅ CHANGED: apply mapping BEFORE using as key
            mapped_name = map_player_name(name, combined_squad) or name

            if mapped_name not in player_dict:
                player_dict[mapped_name] = {
                    'display_name': name_raw,
                    'runs': 0, 'balls': 0, 'fours': 0, 'sixes': 0,
                    'balls_bowled': 0, 'runs_conceded': 0, 'wickets': 0,
                    'fielding_points': 0, 'bat_pos': None,
                    'maiden_bonus': 0
                }

            catches = f.get('catch', 0)
            runouts = f.get('runout', 0)
            stumpings = f.get('stumped', 0)

            player_dict[mapped_name]['fielding_points'] += (
                catches * 8 + runouts * 6 + stumpings * 12
            )

        # ----------------------
        # ✅ FINAL INSERT
        # ----------------------
        for player, stats in player_dict.items():

            # ✅ CHANGED: player is already mapped
            mapped_player = player

            if mapped_player in team1_squad_norm:
                team = team1
                opponent = team2
            elif mapped_player in team2_squad_norm:
                team = team2
                opponent = team1
            else:
                print(f"⚠️ Player not found in squads: {player}")
                continue

            match_count = session.query(func.count(PlayerMatchStats.match_id))\
                .filter(PlayerMatchStats.player_name == mapped_player)\
                .scalar()

            # ------------------
            # CALCULATIONS
            # ------------------
            runs = stats['runs']
            balls = stats['balls']
            fours = stats['fours']
            sixes = stats['sixes']
            wickets = stats['wickets']
            balls_bowled = stats['balls_bowled']
            runs_conceded = stats['runs_conceded']

            batting_points = runs + fours * 1 + sixes * 2
            if runs == 0 and balls > 0:
                batting_points -= 2

            if runs >= 100:
                batting_bonus = 16
            elif runs >= 50:
                batting_bonus = 8
            elif runs >= 30:
                batting_bonus = 4
            else:
                batting_bonus = 0

            sr_bonus = 0
            if balls >= 10:
                sr = (runs / balls) * 100 if balls > 0 else 0
                if sr < 50: sr_bonus = -6
                elif sr < 60: sr_bonus = -4
                elif sr < 70: sr_bonus = -2
                elif sr >= 170: sr_bonus = 6
                elif sr >= 150: sr_bonus = 4
                elif sr >= 130: sr_bonus = 2

            bowling_points = wickets * 25

            if wickets >= 5:
                wicket_bonus = 16
            elif wickets >= 4:
                wicket_bonus = 8
            elif wickets >= 3:
                wicket_bonus = 4
            else:
                wicket_bonus = 0

            eco_bonus = 0
            if balls_bowled >= 12:
                economy = (runs_conceded / balls_bowled) * 6
                if economy < 5: eco_bonus = 6
                elif economy < 6: eco_bonus = 4
                elif economy < 7: eco_bonus = 2
                elif economy > 12: eco_bonus = -6
                elif economy > 11: eco_bonus = -4
                elif economy >= 10: eco_bonus = -2

            lbw_bonus = lbw_bowled_tracker.get(mapped_player, 0)
            lbw_bonus *= 8
            maiden_bonus = stats['maiden_bonus']
            fielding_points = stats['fielding_points']

            fantasy_points = (
                batting_points + batting_bonus + sr_bonus +
                bowling_points + wicket_bonus + eco_bonus +
                lbw_bonus + maiden_bonus + fielding_points
            )

            # ✅ WICKETKEEPER FLAG
            is_wk = mapped_player in WICKETKEEPERS

            player_row = PlayerMatchStats(
                match_id=match_id,
                player_name=mapped_player,
                team=team,
                opponent=opponent,
                runs=runs,
                balls_played=balls,
                fours=fours,
                sixes=sixes,
                balls_bowled=balls_bowled,
                runs_conceded=runs_conceded,
                wickets=wickets,
                fielding_points=fielding_points,
                lbw_bonus=lbw_bonus,
                maiden_bonus=maiden_bonus,
                fantasy_points=fantasy_points,
                batting_position=stats['bat_pos'],
                player_match_number=(match_count or 0) + 1,
                is_wicketkeeper=is_wk
            )

            session.add(player_row)

    session.commit()
    print(f"✅ Match ingested with match_id: {match_id}")