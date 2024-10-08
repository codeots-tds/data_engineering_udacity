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
staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    first_name VARCHAR,
    gender CHAR(1),
    item_in_session INT,
    last_name VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration BIGINT,
    session_id INT,
    song VARCHAR,
    status INT,
    ts BIGINT,
    user_agent VARCHAR,
    user_id INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs INT,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id INT IDENTITY(0,1) NOT NULL PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR NOT NULL,
    song_id VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    session_id INT NOT NULL,
    location VARCHAR NOT NULL,
    user_agent VARCHAR NOT NULL
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id INT NOT NULL PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender CHAR(1),
    level VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    year INT,
    duration FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP PRIMARY KEY,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL
);
""")

# STAGING TABLES

# Staging events copy command
staging_events_copy = ("""
COPY staging_events
FROM {}
IAM_ROLE '{}'
JSON {}
REGION 'us-west-2';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

# Staging songs copy command
staging_songs_copy = ("""
COPY staging_songs
FROM {}
IAM_ROLE '{}'
JSON 'auto'
REGION 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

print(staging_events_copy)
print('-----------')
print(staging_songs_copy)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT
    TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,  -- Convert timestamp to proper format
    se.user_id,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.session_id,
    se.location,
    se.user_agent
FROM staging_events se
JOIN staging_songs ss
    ON se.song = ss.title
    AND se.artist = ss.artist_name
WHERE se.page = 'NextSong';
""")


user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    se.user_id,
    se.first_name,
    se.last_name,
    se.gender,
    se.level
FROM staging_events se
WHERE page = 'NextSong' AND user_id IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT
    ss.song_id,
    ss.title,
    ss.artist_id,
    ss.year,
    ss.duration
FROM staging_songs ss
WHERE ss.song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT DISTINCT
    ss.artist_id,
    ss.artist_name AS name,
    ss.artist_location AS location,
    ss.artist_latitude AS latitude,
    ss.artist_longitude AS longitude
FROM staging_songs ss
WHERE ss.artist_id IS NOT NULL;
""")


time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
    TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
    EXTRACT(hour FROM TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'),
    EXTRACT(day FROM TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'),
    EXTRACT(week FROM TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'),
    EXTRACT(month FROM TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'),
    EXTRACT(year FROM TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'),
    EXTRACT(weekday FROM TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second')
FROM staging_events se
WHERE page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
