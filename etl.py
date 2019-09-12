'''
@author: Phung Binh
@email: psybinh@gmail.com
'''

import os
import pandas as pd
import psycopg2
from sql_queries import insert_to_songplays, insert_to_users, insert_to_songs, insert_to_artists, insert_to_time

DATA_FOLDER = '/home/binhps/de-udacity/data_modeling_with_postgres/MillionSongSample/'
LOG_DATA_FOLDER = DATA_FOLDER + 'log_data/'
SONG_DATA_FOLDER = DATA_FOLDER + 'song_data/'

def connect_db(host, user, password):
    try:
        conn = psycopg2.connect('host={} user={} password={}'.format(host, user, password))
        conn.set_session(autocommit=True)
    except psycopg2.Error as e:
        print(e)
    return conn

def disconnect_db(conn):
    try:
        conn.close()
    except psycopg2.Error as e:
        print(e)

def execute_query(curr, query, values):
    try:
        curr.execute(query, values)
    except psycopg2.Error as e:
        print(e)

# log data
def all_files(_dir):
    for root, dirs, files in os.walk(_dir):
        for file in files:
            path = os.path.join(root, file)
            yield path

def process_song_data(file_df, curr):
    # songs table
    song_data = file_df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data = song_data.values[0]
    execute_query(insert_to_songs, song_data)
    # artists table
    artist_data = file_df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data = artist_data.values[0]
    execute_query(insert_to_artists, artist_data)
    

def process_log_data(file_df, curr):
    df_nextsong = file_df[file_df['page'] == 'NextSong']
    # time table
    datetime = pd.to_datetime(df_nextsong['ts'], unit='ms')
    datetime.apply(lambda row: execute_query(insert_to_time, 
                                             [row.timestamp(), 
                                              row.hour, 
                                              row.day, 
                                              row.dayofweek, 
                                              row.month, 
                                              row.year, 
                                              row.weekday()]), axis=1)
    # users table
    users = df_nextsong[['userId', 'firstName','lastName','gender','level']]
    users.apply(lambda row: execute_query(insert_to_users, row.values), axis=1)
    # songplays table
    for index, row in df_nextsong.iterrows():
        curr.execute(select_song, [row.song, row.artist, row.length])
        results = cur.fetchone()
        
        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None
            
        starttime = pd.to_datetime(row.ts, unit='ms')
        
        songplay_data = (starttime, row.userId, row.level, song_id, artist_id, row.sessionId, row.location, row.userAgent)
        execute_query(insert_to_songplays, songplay_data)
            
if __name__ == '__main__':
    conn = connect_db(host='localhost', user='postgres', password='docker')
    curr = conn.cursor()
    for file_path in all_files(SONG_DATA_FOLDER):
        file = pd.read_json(file_path, lines=True)
        process_song_data(file, curr)
        
    for file_path in all_files(LOG_DATA_FOLDER):
        file = pd.read_json(file_path, lines=True)
        process_log_data(file, curr)
    disconnect_db(conn)