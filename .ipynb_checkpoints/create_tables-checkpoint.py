'''
@author: Phung Binh
@email: psybinh@gmail.com
'''

import psycopg2
from sql_queries import create_db, drop_db
from sql_queries import drop_table_artists, drop_table_songplays, drop_table_songs, drop_table_time, drop_table_users
from sql_queries import create_table_artists, create_table_songplays, create_table_songs, create_table_time, create_table_users

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

def execute_query(curr, query):
    try:
        curr.execute(query)
    except psycopg2.Error as e:
        print(e)

if __name__ == '__main__':
    conn = connect_db(host='localhost', user='postgres', password='docker')
    curr = conn.cursor()
    # create db
    execute_query(curr, drop_db)
    #execute_query(curr, create_db)
    # create table songplays
    execute_query(curr, drop_table_songplays)
    execute_query(curr, create_table_songplays)
    # create table songs
    execute_query(curr, drop_table_songs)
    execute_query(curr, create_table_songs)
    # create table users
    execute_query(curr, drop_table_users)
    execute_query(curr, create_table_users)
    # create table artists
    execute_query(curr, drop_table_artists)
    execute_query(curr, create_table_artists)
    # create table time
    execute_query(curr, drop_table_time)
    execute_query(curr, create_table_time)
    disconnect_db(conn)