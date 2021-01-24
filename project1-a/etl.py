import os
import sys
import glob
import psycopg2
import pandas as pd
from sql_queries import *



def get_files_paths(dir_path):
    """
    Traverses through a given directory path and lists all the .JSON files in that path
    +++++++
    filepath: str -> Relative directory path which contains the files to be processed.path: 
    """
    all_files = []
    for root, dirs, files in os.walk(dir_path):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files



def bulk_copy_data(conn, cursor, dir_path, table_name, cols_labels):
    """
    gathers the record for a given table into a csv formatted object in memory and
    uses the psycopg2 copy command to copy all the data together into the table.
    More efficient than inserting every record individually.
    +++++++
    cur: psycopg2.cursor -> Psycopg2 curors poiting to a Database
    conn: psycopg2.connection -> Connection session to a database
    dir_path: str -> Relative directory path which contains the files to be processed.
    table_name: str -> The postgres table name.
    column_names: list -> List of columns that need to be inserted into a table
    """
    # get a list of json files to process
    file_list = get_files_paths(dir_path)

    # Define a lambda to read all the files, pick the columns for the given table and conver to csv
    song_file_lambda_func = lambda file_name: pd.read_json(file_name,typ='series').T[cols_labels].to_csv(sep='\t', header=False, index=False)
    log_file_lambda_func = lambda file_name: pd.read_json(file_name,typ='series')

    if table_name == "songs":
        # using a set would remove any duplicate data as well
        song_data = set(map(song_file_lambda_func, file_list))
    elif table_name == "artists":
        artist_data= set(map(song_file_lambda_func, file_list))
    elif table_name == "users":
        artist_data= set(map(lambda_func, file_list))
    elif table_name == "artists":
        artist_data= set(map(lambda_func, file_list))
    elif table_name == "artists":
        artist_data= set(map(lambda_func, file_list))
    pass

def process_song_file(cur, filepath):
    """
    Processes a song file and extracts all the data within it. 
    It separates the data into song data and artist data then inserts them into their
    respective tables in the Postgres database.
    +++++++
    cur: psycopg2.cursor -> Psycopg2 curors poiting to a Database
    filepath: str -> Relative directory path which contains the files to be processed.
    Return: None.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 
                        'artist_latitude', 'artist_longitude']]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Processes a log file and extracts all the data within it. 
    It separates the data into time data, user data, and songplay records 
    and then inserts them into their
    respective tables in the Postgres database.
    +++++++
    cur: psycopg2.cursor -> Psycopg2 curors poiting to a Database
    filepath: str -> Relative directory path which contains the files to be processed.
    Return: None.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # Compile each record into the time columns
    time_data_list = []
    for row in t:
        time_data_list.append([row, row.hour, row.day, 
                                row.week, row.month, row.year, row.weekday()])
    column_labels = ("start_time", "hour", "day", "week", "month", "year", "weekday")
    time_df = time_df = pd.DataFrame(time_data_list, columns=column_labels)
    
    # insert time data records
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, row.tolist())

    # separate used data columns
    user_df = df.sort_values('ts')[['userId', 'firstName', 'lastName', 'gender', 'level']]
    
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
        songplay_data = [pd.to_datetime(row.ts, unit="ms"), row.userId, row.level, songid, artistid, 
                        row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, dir_path, func):
    """
    Porocesses the data in all the files in a given directory using a func
    +++++++
    cur: psycopg2.cursor -> Psycopg2 curors poiting to a Database
    conn: psycopg2.connection -> Connection session to a database
    dir_path: str -> Relative directory path which contains the files to be processed.
    func: function -> Function used to process files within filepath
    Return: None
    """
    # get all files matching extension from directory
    all_files = get_files_paths(dir_path)

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, dir_path))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main(argv):
    """
    Acts as the Entry point of the pipeline. 
    Sets up Postres connection, and cursor for data read and insertion into the database.
    It will then read all song and log files, then it will call each ETL operation for each dimention of the schema.
    +++++++
    argv: list -> System arguements passed when running this python script. 
            If nothing else passed, the file name a.k.a etl.py is always the first value. 
            Additiona flags can be passed to influence the behaviour of the code.
            1. -bulkCopy: If this flag is passed, the code will then  use Psycopg2's copy_from method
                            to bulk copy the entire records to the database. The default behaviour without this
                            flag is to loop through each record and insert it into the database.
    return: None.
    """
    conn = psycopg2.connect("host=127.0.0.1dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    if len(argv) > 1 and argv[1] =="-bulkCopy":
        pass
    else:
        process_data(cur, conn, filepath='data/song_data', func=process_song_file)
        process_data(cur, conn, filepath='data/log_data', func=process_log_file) 

    conn.close()


if __name__ == "__main__":
    main()