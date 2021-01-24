# DROP TABLES

songplay_table_drop = "DROP TABLE if exists songplays"
user_table_drop = "DROP TABLE if exists users"
song_table_drop = "DROP TABLE if exists songs"
artist_table_drop = "DROP TABLE if exists artists"
time_table_drop = "DROP TABLE if exists time"

# CREATE TABLES
base_subquery_create = "Create Table If Not Exists "
songplay_table_create = (base_subquery_create + """songplays (
    songplay_id SERIAL Primary Key,
    start_time timestamp Not Null,
    user_id int Not Null,
    level varchar Not Null,
    song_id varchar,
    artist_id varchar,
    session_id int Not Null, 
    location varchar Not Null,
    user_agent varchar Not Null);""")

user_table_create = (base_subquery_create + """users (
    user_id int Primary Key,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar);""")

song_table_create = (base_subquery_create + """songs (
    song_id varchar Primary Key,
    title varchar,
    artist_id varchar,
    year int,
    duration float);""")

artist_table_create = (base_subquery_create + """artists (
    artist_id varchar Primary Key,
    name varchar Not Null,
    location varchar,
    latitude float,
    longitude float);""")

time_table_create = (base_subquery_create + """time (
    start_time timestamp Primary Key,
    hour int NOT NULL,
    day int NOT NULL,
    week int NOT NULL,
    month int NOT NULL,
    year int NOT NULL,
    weekday int NOT NULL);""")

# INSERT RECORDS
conflict_subquery = "On Conflict Do Nothing;"
songplay_table_insert = ("""Insert Into songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent) 
    Values (%s, %s, %s, %s, %s, %s, %s, %s) """ + conflict_subquery)

user_table_insert = ("""Insert Into users (
    user_id, 
    first_name, 
    last_name, 
    gender, 
    level) 
    Values (%s, %s, %s, %s, %s) """ + conflict_subquery)

song_table_insert = ("""INSERT Into songs (
    song_id, 
    title, 
    artist_id, 
    year, 
    duration) 
    Values (%s, %s, %s, %s, %s) """ + conflict_subquery)

artist_table_insert = ("""Insert Into artists (
    artist_id, 
    name, 
    location, 
    latitude, 
    longitude) 
    Values (%s, %s, %s, %s, %s) """ + conflict_subquery)


time_table_insert = ("""Insert Into time (
    start_time, 
    hour, 
    day, 
    week, 
    month, 
    year, 
    weekday) 
    Values (%s,%s,%s,%s,%s,%s,%s) """ + conflict_subquery)

# FIND SONGS

song_select = ("""
                select song_id, artists.artists_id 
                From songs Join artists On songs.artist_id=artists.artist_id
                Where
                    title=%s and
                    name=%s and
                    duration=%s;""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]