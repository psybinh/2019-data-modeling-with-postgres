'''
@author: Phung Binh
@email: psybinh@gmail.com
'''

# DB queries
create_db = '''CREATE DATABASE model'''
drop_db = '''DROP DATABASE IF EXISTS model'''

# Drop tables
drop_table_songplays = '''DROP TABLE IF EXISTS songplays'''
drop_table_users = '''DROP TABLE IF EXISTS users'''
drop_table_songs = '''DROP TABLE IF EXISTS songs'''
drop_table_artists = '''DROP TABLE IF EXISTS artists'''
drop_table_time = '''DROP TABLE IF EXISTS time'''

# Create tables
create_table_songplays = '''
CREATE TABLE songplays(
    songplay_id serial PRIMARY KEY, 
    start_time timestamp, 
    user_id int, 
    level varchar, 
    song_id varchar, 
    artist_id varchar, 
    session_id int, 
    location varchar, 
    user_agent varchar)
'''

create_table_users = '''
CREATE TABLE users( 
    user_id int PRIMARY KEY, 
    first_name varchar, 
    last_name varchar, 
    gender varchar, 
    level varchar)
'''

create_table_songs = '''
CREATE TABLE songs( 
    song_id varchar PRIMARY KEY, 
    title varchar, 
    artist_id varchar, 
    year int, 
    duration float)
'''

create_table_artists = '''
CREATE TABLE artists( 
    artist_id varchar PRIMARY KEY, 
    name varchar, 
    location varchar, 
    latitude float, 
    longitude float)
'''

create_table_time = '''
CREATE TABLE time( 
    start_time timestamp PRIMARY KEY, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int)
'''

# Insert
insert_to_songplays = '''
INSERT INTO songplays
    (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
'''

insert_to_users = '''
INSERT INTO users VALUES
    (user_id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s)
'''

insert_to_songs = '''
INSERT INTO songs VALUES
    (song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s)
'''

insert_to_artists = '''
INSERT INTO artists VALUES
    (artist_id, name, location, latitude, longitude)
VALUES (%s, %s, %s, %s, %s)
'''

insert_to_time = '''
INSERT INTO time VALUES
    (start_time, hour, day, week, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
'''

# Select
select_song = '''
SELECT songs.song_id, artists.artist_id
FROM 
    songs JOIN artists ON songs.artist_id = artists.artist_id
WHERE
    songs.title = %s AND artists.name = %s AND songs.duration = %s
'''