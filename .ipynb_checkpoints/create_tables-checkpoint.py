import psycopg2

conn = None

def connect_db(host, user, password):
    try:
        conn = psycopg2.connect('host={} user={} password={}'.format(host, user, password))
    except psycopg2.Error as e:
        print(e)
    return conn

def execute_query(query):
    conn.set_session(autocommit=True)
    try:
        conn.execute(query)
    except psycopg2.Error as e:
        print(e)

def create_table_songplays():
    query = 'CREATE TABLE IF NOT EXITS songplays '
    query += '(songplay_id, \
            start_time, \
            user_id, \
            level, \
            song_id, \
            artist_id, \
            session_id, \
            location, \
            user_agent)'''

def disconnect_db():
    try:
        conn.close()
    except psycopg2.Error as e:
        print(e)

if __name__ == '__main__':
    conn = connect_db(host='localhost', user='postgres', password='docker')
    disconnect_db()