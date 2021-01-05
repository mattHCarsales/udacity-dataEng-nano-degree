# DROP TABLES

songplay_table_drop = ""
user_table_drop = ""
song_table_drop = ""
artist_table_drop = ""
time_table_drop = ""

# CREATE TABLES

songplay_table_create = ("""Create Table If Not Exists songplays (
    songplay_id SERIAL Primary Key,
    start_time timestamp Not Null,
    user_id int Not Null,
    level varchar,
    song_id varchar Not Null,
    artist_id varchar Not Null,
    session_id int, 
    location varchar,
    user_agent varchar);""")

user_table_create = ("""Create Table If Not Exists users (
    user_id int Primary Key,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar);""")

song_table_create = ("""Create Table If Not Exists songs (
    song_id varchar Primary Key,
    title varcar Not Null,
    artist_id varchar,
    year int,
    duration float);""")

artist_table_create = ("""Create Table If Not Exists artists (
    artist_id varchar Primary Key,
    name varchar,
    location varchar,
    latitude float,
    longitude float);""")

time_table_create = ("""Create Table If Not Exists time (
    start_time timestamp Primary Key,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday varchar);""")

# INSERT RECORDS

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")


time_table_insert = ("""
""")

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]