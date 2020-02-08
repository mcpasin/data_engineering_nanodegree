import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events(
    artist             VARCHAR,
    auth               VARCHAR,
    firstName          VARCHAR,
    gender             VARCHAR,
    itemInSession	   INTEGER,
    lastName           VARCHAR,
    length	           FLOAT, 
    level              VARCHAR,
    location           VARCHAR,	
    method             VARCHAR,
    page               VARCHAR,	
    registration       VARCHAR,	
    sessionId	       BIGINT,
    song               VARCHAR,
    status             INT,
    ts                 TIMESTAMP,
    userAgent          VARCHAR,	
    userId             INTEGER
    );
""")

staging_songs_table_create = ("""CREATE TABLE staging_songs(
    song_id             VARCHAR,
    num_songs           INT,
    artist_id           VARCHAR,
    artist_latitude     FLOAT,
    artist_longitude    FLOAT,
    artist_location     VARCHAR,
    artist_name         VARCHAR,
    title               VARCHAR,
    duration            FLOAT,
    year                INTEGER
    );
""")

songplay_table_create = ("""CREATE TABLE songplays(
    songplay_id         INT IDENTITY(0,1),
    start_time          TIMESTAMP NOT NULL DISTKEY SORTKEY,
    user_id             INT NOT NULL,
    level               VARCHAR,
    song_id             VARCHAR NOT NULL,
    artist_id           VARCHAR NOT NULL,
    session_id          BIGINT,
    location            VARCHAR,
    user_agent          VARCHAR,
    PRIMARY KEY (songplay_id)) 
""")

user_table_create = ("""CREATE TABLE users(
    user_id INTEGER SORTKEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    gender VARCHAR,
    level VARCHAR,
    PRIMARY KEY (user_id)) DISTSTYLE AUTO
""")

song_table_create = ("""CREATE TABLE songs(
    song_id VARCHAR SORTKEY,
    title VARCHAR,
    artist_id VARCHAR NOT NULL,
    year INTEGER,
    duration FLOAT,
    PRIMARY KEY (song_id)) DISTSTYLE AUTO
""")

artist_table_create = ("""CREATE TABLE artists(
    artist_id VARCHAR SORTKEY,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT,
    PRIMARY KEY (artist_id)) DISTSTYLE AUTO
""")

time_table_create = ("""CREATE TABLE time(
    start_time TIMESTAMP DISTKEY SORTKEY,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER,
    PRIMARY KEY (start_time))
""")

# STAGING TABLES

# compudate off statupdate off improves performance of COPY operation

staging_events_copy = ("""copy staging_events from '{}'
 credentials 'aws_iam_role={}'
 region 'us-west-2' 
 COMPUPDATE OFF STATUPDATE OFF
 format as JSON '{}'
 timeformat as 'epochmillisecs';
""").format(config.get('S3','LOG_DATA'),
            config.get('IAM_ROLE', 'ARN'),
            config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""copy staging_songs from '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2' 
    COMPUPDATE OFF STATUPDATE OFF 
    format as JSON 'auto'
""").format(config.get('S3','SONG_DATA'), 
            config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    SELECT 
        DISTINCT (e.ts) as start_time, 
        e.userId as user_id, 
        e.level as level,
        s.song_id as song_id,
        s.artist_id as artist_id,
        e.sessionId as session_id,
        e.location as location,
        e.userAgent as user_agent
    FROM staging_events e 
    LEFT JOIN staging_songs s
    ON (e.song = s.title AND e.artist = s.artist_name)
    WHERE e.page = 'NextSong'
        AND s.song_id IS NOT NULL
    --limit 1000;
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)  
    SELECT 
        DISTINCT (userId) as user_id,
        firstName as first_name,
        lastName as last_name,
        gender, 
        level
    FROM staging_events
    WHERE page = 'NextSong'
        AND user_id is not null
    --limit 1000;
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT  
        DISTINCT(song_id) AS song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
    --limit 1000;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT  
        DISTINCT(artist_id) AS artist_id,
        artist_name         AS name,
        artist_location     AS location,
        artist_latitude     AS latitude,
        artist_longitude    AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
    --limit 1000;
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  
        DISTINCT(start_time)                AS start_time,
        EXTRACT(hour FROM start_time)       AS hour,
        EXTRACT(day FROM start_time)        AS day,
        EXTRACT(week FROM start_time)       AS week,
        EXTRACT(month FROM start_time)      AS month,
        EXTRACT(year FROM start_time)       AS year,
        EXTRACT(dayofweek FROM start_time)  as weekday
    FROM songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
