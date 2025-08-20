import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import nfl_data_py as nfl

def scrape_nfl_odds(year):
    url = f'https://www.sportsoddshistory.com/nfl-game-season/?y={year}'
    headers = {
        'User-Agent': "Mozilla/5,0"
    }

    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')
    tables = soup.find_all("table", {"class": "soh1"})

    weekly_data = []
    i=1
    for table in tables:
        headers = table.find_all('th')
        headers = [[header.text.strip() for header in headers]][0]
        if len(headers) == 11:
            week_title = f'week_{i}'
            i+=1
            rows = table.find_all("tr")[1:]
            for row in rows:
                cols = [col.text.strip() for col in row.find_all("td")]
                weekly_data.append([year] + [week_title] + cols)

    headers = ['season', 'week', 'day', 'date', 'time_ET', 'favorite_home_or_away', 'favorite', 'score', 'spread', 'underdog_home_or_away', 'underdog', 'over_under', 'notes']
    df = pd.DataFrame(weekly_data, columns=headers)
    return df


def transform_scraped_odds(start_year, end_year):
    df_full = pd.DataFrame(columns=['week', 'day', 'date', 'time_ET', 'favorite_home_or_away', 'favorite', 'score', 'spread', 'underdog_home_or_away', 'underdog', 'over_under', 'notes'])
    for year in range(start_year, end_year):
        temp_df = scrape_nfl_odds(year)
        df_full = pd.concat([df_full, temp_df], ignore_index=True)
        print(year)


    # df_full.to_csv('spread_data.csv', index=False)

    df = df_full.copy()
    df = df[df['day'].str.len() == 3]



    df['favorite_score'] = df['score'].str.split(' ').str[1].str.split('-').str[0].astype(float)
    df['underdog_score'] = df['score'].str.split(' ').str[1].str.split('-').str[1].astype(float)
    df['spread_result'] = df['spread'].str.split(' ').str[0]
    df['spread_raw'] = df['spread'].str.split(' ').str[1]
    df['spread_raw'] = np.where(df['spread_raw'] == 'PK', 0, df['spread_raw'])
    df['spread_actual'] = df['underdog_score'] - df['favorite_score']
    df['ou_result'] = df['over_under'].str.split(' ').str[0]
    df['ou_raw'] = df['over_under'].str.split(' ').str[1]
    df['ou_actual'] = df['favorite_score'] + df['underdog_score']


    df['game_id'] = (
        df_full['season'].astype(str) + '_' + df['week'] + '_' + df['favorite'].str.replace(' ', '_') + '_' + df['underdog'].str.replace(' ', '_')
    )




    melted_df = pd.melt(
        df, 
        id_vars=[col for col in df.columns if col not in ['favorite', 'underdog']],
        value_vars=['favorite', 'underdog'],
        var_name='role',
        value_name='team'
    )


    melted_df['home_or_away'] = np.where(
        ((melted_df['role'] == 'favorite') & (melted_df['favorite_home_or_away'] == '@'))
        | ((melted_df['role'] == 'underdog') & (melted_df['underdog_home_or_away'] == '@')),
        'away',
        'home'
    )

    melted_df['team_score'] = np.where(
        melted_df['role'] == 'favorite',
        melted_df['favorite_score'],
        melted_df['underdog_score']
    )
    melted_df['opponent_score'] = np.where(
        melted_df['role'] == 'favorite',
        melted_df['underdog_score'],
        melted_df['favorite_score']
    )




    melted_df['season'] = melted_df['season'].round(0).astype(int).astype(str)

    melted_df.head()
    df_final = melted_df[
        [
            'game_id',
            'season',
            'date',
            'week',
            'team',
            'role',
            'home_or_away',
            'team_score',
            'opponent_score',
            'favorite_score',
            'underdog_score',
            'spread',
            'spread_result',
            'spread_actual',
            'spread_raw',
            'over_under',
            'ou_result',
            'ou_raw',
            'ou_actual'
        ]
    ].copy()

    return df_final



def get_weekly_stats(start_year, end_year):
    df = nfl.import_weekly_data(range(start_year, end_year), downcast=True)

    df_team_weekly = df.groupby(['recent_team', 'opponent_team', 'season', 'week'])[[
        'passing_yards',
        'passing_tds',
        'interceptions',
        'sacks',
        'rushing_yards',
        'rushing_tds'
    ]].sum().reset_index()


    weekly_copy = df_team_weekly.rename(columns={
        'passing_yards': 'allowed_passing_yards',
        'passing_tds' : 'allowed_passing_tds',
        'interceptions' : 'ints',
        'sacks': 'sacks',
        'rushing_yards': 'allowed_rushing_yards',
        'rushing_tds': 'allowed_rushing_tds'
    }).copy()

    weekly_merged = pd.merge(df_team_weekly, weekly_copy, 
        how='left', 
        left_on=['opponent_team', 'season', 'week'], 
        right_on=['recent_team', 'season', 'week']
    ).rename(columns={
        'recent_team_x': 'team',
        'opponent_team_x': 'opponent'
    }).drop(['recent_team_y', 'opponent_team_y'], axis=1)

    return weekly_merged




team_mapping = {
    'Arizona Cardinals': 'ARI',
    'Atlanta Falcons': 'ATL',
    'Baltimore Ravens': 'BAL',
    'Buffalo Bills': 'BUF',
    'Carolina Panthers': 'CAR',
    'Chicago Bears': 'CHI',
    'Cincinnati Bengals': 'CIN',
    'Cleveland Browns': 'CLE',
    'Dallas Cowboys': 'DAL',
    'Denver Broncos': 'DEN',
    'Detroit Lions': 'DET',
    'Green Bay Packers': 'GB',
    'Houston Texans': 'HOU',
    'Indianapolis Colts': 'IND',
    'Jacksonville Jaguars': 'JAX', 
    'Kansas City Chiefs': 'KC',
    'Las Vegas Raiders': 'LV',
    'Los Angeles Chargers': 'LAC',
    'Los Angeles Rams': 'LAR',
    'Miami Dolphins': 'MIA',
    'Minnesota Vikings': 'MIN',
    'New England Patriots': 'NE',
    'New Orleans Saints': 'NO',
    'New York Giants': 'NYG',
    'New York Jets': 'NYJ',
    'Oakland Raiders': 'LV',
    'Philadelphia Eagles': 'PHI',
    'Pittsburgh Steelers': 'PIT',
    'San Diego Chargers': 'LAC',
    'San Francisco 49ers': 'SF',
    'Seattle Seahawks': 'SEA',
    'St Louis Rams': 'LA',
    'Tampa Bay Buccaneers': 'TB',
    'Tennessee Titans': 'TEN',
    'Washington Commanders': 'WAS',
    'Washington Football Team': 'WAS',
    'Washington Redskins': 'WAS'
}



df_odds = transform_scraped_odds(2000, 2025)
df_stats = get_weekly_stats(2000, 2025)


df_opponents = df_odds[['team', 'game_id']].rename(columns={'team': 'opponent'})
df_odds = pd.merge(df_odds, df_opponents, how='inner', on='game_id') 
df_odds = df_odds[df_odds['team'] != df_odds['opponent']]

df_odds['team_abr'] = df_odds['team'].map(team_mapping)
df_odds['opponent_abr'] = df_odds['opponent'].map(team_mapping)
df_odds['week_num'] = df_odds['week'].str.split('_').str[1]


df_stats['season'] = df_stats['season'].astype(str)
df_stats['week'] = df_stats['week'].astype(str)

odds_and_stats_merged = pd.merge(df_odds, df_stats, how='left', 
    left_on=['season', 'week_num', 'team_abr', 'opponent_abr'],
    right_on=['season', 'week', 'team', 'opponent'],
).reset_index()
