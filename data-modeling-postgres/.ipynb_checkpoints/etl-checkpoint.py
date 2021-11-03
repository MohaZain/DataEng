import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import datetime


def process_song_file(cur, filepath):
    """
    This procedure processes a song file
    It extracts the song and artists information 
    in order to store it into the songs table and artists table.

    INPUTS:
    * cur the cursor variable
    * filepath the file path to the song file
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = []
    for song in df[['song_id',
                    'title',
                    'artist_id',
                    'year',
                    'duration']].values:
        song_data = song
    song_data = list(song_data)

    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = []
    for art in df[['artist_id',
                   'artist_name',
                   'artist_location',
                   'artist_latitude',
                   'artist_longitude']].values:
        artist_data = art
    artist_data = list(artist_data)

    cur.execute(artist_table_insert, artist_data)



def process_log_file(cur, filepath):
    """
    This procedure processes a log file
    It extracts the time and users information 
    in order to store it into the time table , users table and song_play table.
    
    INPUTS:
    * cur the cursor variable
    * filepath the file path to the log file
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    """
    Process the Time
    """
    t = []
    temp = {}
    for time in df['ts']:
        date = datetime.datetime.fromtimestamp(time / 1000.0)
        temp = {
            'timestamp': date,
            'hour': date.hour,
            'day': date.day,
            'week_of_year': date.isocalendar()[1],
            'month': date.month,
            'year': date.year,
            'weekday': date.isocalendar()[2]
        }
        t.append(temp)
    t = pd.DataFrame(t)
    t = t.reindex(['timestamp', 'hour', 'day', 'week_of_year',
                  'month', 'year', 'weekday'], axis=1)

    # insert time data records
    time_df = t
    """
    Process User information
    """
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    """
    Process songplay information
    """
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
        time = datetime.datetime.fromtimestamp(row['ts'] / 1000.0)
        songplay_data = (
            time,
            row['userId'],
            row['level'],
            songid,
            artistid,
            row['sessionId'],
            row['location'],
            row['userAgent'])
        cur.execute(songplay_table_insert, songplay_data)



def process_data(cur, conn, filepath, func):
    """
    This procedure processes log and song functions

    INPUTS:
    * cur the cursor variable
    * conn the connection to the DB
    * filepath the file path to the log file
    * fun to process log and song data
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
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
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()