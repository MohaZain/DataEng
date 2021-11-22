import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays cascade"
user_table_drop = "DROP TABLE IF EXISTS users cascade"
song_table_drop = "DROP TABLE IF EXISTS songs cascade"
artist_table_drop = "DROP TABLE IF EXISTS artists cascade"
time_table_drop = "DROP TABLE IF EXISTS time cascade"

# CREATE TABLES
staging_events_table_create= (""" 
                        CREATE TABLE IF NOT EXISTS staging_events 
                                (artist varchar, 
                                auth varchar , 
                                firstName varchar , 
                                gender varchar, 
                                itemInSession int, 
                                lastName varchar, 
                                length NUMERIC, 
                                level varchar,
                                location varchar,
                                method varchar,
                                page varchar,
                                registration NUMERIC,
                                sessionId int ,
                                song varchar,
                                status int,
                                ts timestamp,
                                userAgent varchar,
                                userId int);
""")

staging_songs_table_create = ("""
                    CREATE TABLE IF NOT EXISTS staging_songs 
                               (
                                num_songs int,
                                artist_id varchar,
                                artist_latitude numeric,
                                artist_longitude numeric,
                                artist_location varchar,
                                artist_name varchar,
                                song_id varchar,
                                title varchar,
                                duration numeric,
                                year int)
""")

songplay_table_create = ("""
                                CREATE TABLE IF NOT EXISTS songplays 
                                (songplay_id int IDENTITY(0,1) PRIMARY KEY , 
                                start_time TIMESTAMP , 
                                user_id int NOT NULL, 
                                level varchar , 
                                song_id varchar, 
                                artist_id varchar, 
                                session_id varchar, 
                                location varchar, 
                                user_agent varchar);
""")

user_table_create = ("""
                                CREATE TABLE IF NOT EXISTS users 
                                (user_id int PRIMARY KEY, 
                                first_name varchar, 
                                last_name varchar, 
                                gender varchar, 
                                level varchar);
""")

song_table_create = ("""
                                CREATE TABLE IF NOT EXISTS songs 
                                (song_id varchar PRIMARY KEY,
                                title varchar,
                                artist_id varchar,
                                year int,
                                duration NUMERIC);
""")

artist_table_create = ("""
                                CREATE TABLE IF NOT EXISTS artists 
                                (artist_id varchar PRIMARY KEY, 
                                name varchar,
                                location varchar,
                                latitude NUMERIC,
                                longitude NUMERIC);
""")

time_table_create = ("""
                                CREATE TABLE IF NOT EXISTS time 
                                (start_time TIMESTAMP PRIMARY KEY, 
                                hour int, 
                                day int, 
                                week int, 
                                month int, 
                                year int, 
                                weekday int);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    region 'us-west-2'
    json {}
    timeformat as 'epochmillisecs' """).format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    region 'us-west-2'
    json 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays ( 
                                start_time, 
                                user_id, 
                                level, 
                                song_id, 
                                artist_id, 
                                session_id,
                                location,
                                user_agent) 
                                (SELECT SE.ts, SE.userId, SE.level, SS.song_id,
                                SS.artist_id, SE.sessionId, SS.artist_location, SE.userAgent FROM 
                                staging_events SE, 
                                staging_songs SS
                                WHERE 
                                    SE.song = SS.title
                                    AND SE.artist = SS.artist_name
                                )
""")

user_table_insert = ("""INSERT INTO users (user_id, 
                                first_name, 
                                last_name, 
                                gender, 
                                level) 
                                (SELECT userId, firstName, lastName, gender, level
                                FROM staging_events 
                                     WHERE userId is not null)
""")

song_table_insert = ("""INSERT INTO songs (song_id, 
                                title, 
                                artist_id, 
                                year, 
                                duration) 
                                (SELECT song_id, title, artist_id, year, duration 
                                FROM staging_songs
                                WHERE song_id is not null)
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, 
                                name, 
                                location, 
                                latitude, 
                                longitude) 
                                (SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude 
                                FROM staging_songs
                                WHERE artist_id is not null)
""")


time_table_insert = ("""INSERT INTO time (start_time, 
                                hour, 
                                day, 
                                week, 
                                month,
                                year,
                                weekday) 
                                (SELECT ts , 
                                EXTRACT(hour FROM ts), 
                                EXTRACT(day FROM ts),
                                EXTRACT(week FROM ts),
                                EXTRACT(month FROM ts),
                                EXTRACT(year FROM ts),
                                EXTRACT(week FROM ts)
                                FROM staging_events WHERE ts is not null)
                                
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
