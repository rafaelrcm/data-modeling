import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - Read the file in the filepath (data/song_data)
    
    - Populate the songs dim table
    
    - Populate the artists dim table
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - Read the file in the filepath (data/log_data)
    
    - Populate the time dim table
    
    - Populate the users dim table
    
    - Populate the songplays fact table
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    next_sound_df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t_dt_start_time = ()
    t_dt_hour = ()
    t_dt_day= ()
    t_dt_week= ()
    t_dt_month = ()
    t_dt_year = ()
    t_dt_weekday = ()

    for next_sound in next_sound_df['ts'].values.tolist():
        t = pd.Series(pd.date_range(pd.to_datetime(next_sound, unit='ms'), periods=1, freq="s"))
        t_dt_start_time += (next_sound,)
        t_dt_hour += (int(t.dt.hour),)
        t_dt_day += (int(t.dt.day),)
        t_dt_week += (int(t.dt.week),)
        t_dt_month += (int(t.dt.month),)
        t_dt_year += (int(t.dt.year),)
        t_dt_weekday += (int(t.dt.weekday),)
    
    # insert time data records
    time_data = ()
    time_data = (t_dt_start_time, t_dt_hour, t_dt_day, t_dt_week, t_dt_month, t_dt_year, t_dt_weekday)
    column_labels = ()
    column_labels = ('start_time', 'hours', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        start_time = pd.Series(pd.date_range(pd.to_datetime(row.ts, unit='ms'), periods=1, freq="s"))
        
        if row.userId and row.ts:
            songplay_data = (start_time[0], row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
            cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - Get all files from directory (filepath)
    
    - Call the funcions (func) in interate
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    - Read the file in the filepath (data/song_data) and Creates all tables needed. 
    
    - Read the file in the filepath (data/log_data) and Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()