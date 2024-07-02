import configparser

# Lendo as configurações do arquivo dwh.cfg
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get('S3', 'LOG_DATA')
ARN = config.get('IAM_ROLE', 'ARN')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

# Drop Tables
drop_songplays = "DROP TABLE IF EXISTS songplays"
drop_users = "DROP TABLE IF EXISTS users"
drop_songs = "DROP TABLE IF EXISTS songs"
drop_artists = "DROP TABLE IF EXISTS artists"
drop_time = "DROP TABLE IF EXISTS time"
drop_staging_events = "DROP TABLE IF EXISTS staging_events"
drop_staging_songs = "DROP TABLE IF EXISTS staging_songs"

# Create Tables
create_staging_events = """
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INT,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration BIGINT,
    sessionId INT,
    song VARCHAR,
    status INT,
    ts BIGINT,
    userAgent VARCHAR,
    userId INT
);
"""

create_staging_songs = """
CREATE TABLE IF NOT EXISTS staging_songs (
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
"""

create_songplays = """
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR
);
"""

create_users = """
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR
);
"""

create_songs = """
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR,
    year INT,
    duration FLOAT
);
"""

create_artists = """
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT
);
"""

create_time = """
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
);
"""

# Copy tables (staging)
copy_staging_events = f"""
COPY staging_events
FROM '{LOG_DATA}'
CREDENTIALS 'aws_iam_role={ARN}'
FORMAT AS JSON '{LOG_JSONPATH}'
REGION 'us-west-2';
"""

copy_staging_songs = f"""
COPY staging_songs
FROM '{SONG_DATA}'
CREDENTIALS 'aws_iam_role={ARN}'
FORMAT AS JSON 'auto'
REGION 'us-west-2';
"""

# Insert tables
insert_songplays = """
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
       se.userId,
       se.level,
       ss.song_id,
       ss.artist_id,
       se.sessionId,
       se.location,
       se.userAgent
FROM staging_events se
JOIN staging_songs ss ON (se.song = ss.title AND se.artist = ss.artist_name)
WHERE se.page = 'NextSong';
"""

insert_users = """
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId, firstName, lastName, gender, level
FROM staging_events
WHERE page = 'NextSong';
"""

insert_songs = """
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs;
"""

insert_artists = """
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs;
"""

insert_time = """
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT start_time,
       EXTRACT(hour FROM start_time),
       EXTRACT(day FROM start_time),
       EXTRACT(week FROM start_time),
       EXTRACT(month FROM start_time),
       EXTRACT(year FROM start_time),
       EXTRACT(weekday FROM start_time)
FROM songplays;
"""

# Lists of queries
drop_tables = [
    drop_songplays,
    drop_users,
    drop_songs,
    drop_artists,
    drop_time,
    drop_staging_events,
    drop_staging_songs
]

create_tables = [
    create_staging_events,
    create_staging_songs,
    create_songplays,
    create_users,
    create_songs,
    create_artists,
    create_time
]

copy_table_queries = [
    copy_staging_events,
    copy_staging_songs
]

insert_table_queries = [
    insert_songplays,
    insert_users,
    insert_songs,
    insert_artists,
    insert_time
]
