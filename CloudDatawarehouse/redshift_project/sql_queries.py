import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# GLOBAL VARIABLES
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_PATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA =  config.get("S3", "SONG_DATA")
IAM_ROLE = config.get("IAM_ROLE", "ARN")

# DROP TABLES

staging_event_table_drop = "DROP TABLE IF EXISTS stg_event"
staging_song_table_drop = "DROP TABLE IF EXISTS stg_song"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay"
user_table_drop = "DROP TABLE IF EXISTS dim_user"
song_table_drop = "DROP TABLE IF EXISTS dim_song"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE STAGING TABLES

staging_event_table_create = ("""
CREATE TABLE IF NOT EXISTS stg_event
(
   artist          VARCHAR(500),
   auth            VARCHAR(20), 
   firstName       VARCHAR(500),
   gender          CHAR(1),   
   itemInSession   INTEGER,
   lastName        VARCHAR(500),
   length          FLOAT,
   level           VARCHAR(10), 
   location        VARCHAR(500),
   method          VARCHAR(20),
   page            VARCHAR(500),
   registration    BIGINT,
   sessionId       INTEGER,
   song            VARCHAR(100),
   status          INTEGER,
   ts              TIMESTAMP,
   userAgent       VARCHAR(500),
   userId          INTEGER
);
""")

staging_song_table_create = ("""
CREATE TABLE IF NOT EXISTS stg_song
(
   song_id            VARCHAR(20),
   num_songs          INTEGER,
   title              VARCHAR(500),
   artist_id          VARCHAR(20),
   artist_name        VARCHAR(500),
   artist_location    VARCHAR(100),
   artist_latitude    FLOAT,
   artist_longitude   FLOAT,
   year               INTEGER,
   duration           FLOAT
);
""")

# CREATE FACT TABLE
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplay
(
   songplay_Key         INTEGER IDENTITY(0,1) PRIMARY KEY,
   start_time           TIMESTAMP NOT NULL,
   user_id              INTEGER NOT NULL REFERENCES dim_user (user_id),
   level                VARCHAR(25),
   song_id              VARCHAR(100) REFERENCES dim_song (song_id),
   artist_id            VARCHAR(100) REFERENCES dim_artist (artist_id),
   session_id           INTEGER NOT NULL,
   location             VARCHAR(500),
   user_agent           VARCHAR(500)
)DISTSTYLE EVEN 
COMPOUND SORTKEY(user_id, song_id, artist_id);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_user(
    user_id         INTEGER PRIMARY KEY SORTKEY,
    first_name      VARCHAR(500),
    last_name       VARCHAR(500),
    gender          CHAR(1),
    level           VARCHAR(10));
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_song(
     song_id     VARCHAR(20) PRIMARY KEY,
     title       VARCHAR(500) NOT NULL,
     artist_id   VARCHAR(20) NOT NULL DISTKEY REFERENCES dim_artist (artist_id),
     year        INTEGER NOT NULL,
     duration    DECIMAL(15,5) NOT NULL);
""")


artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artist(
     artist_id          VARCHAR PRIMARY KEY SORTKEY distkey,
     name               VARCHAR(500) NOT NULL,
     location           VARCHAR(500),
     latitude           FLOAT,
     longitude          FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time(
     start_time    TIMESTAMP PRIMARY KEY sortkey,
      hour          INTEGER NOT NULL,
      day           INTEGER NOT NULL,
      week          INTEGER NOT NULL,
      month         INTEGER NOT NULL,
      year          INTEGER NOT NULL,
      weekday       INTEGER NOT NULL
);
""")

# STAGING TABLES

staging_event_copy = ("""
    COPY stg_event FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON {};
""").format(LOG_DATA, IAM_ROLE, LOG_PATH)

staging_song_copy = ("""
    COPY stg_song FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    FORMAT AS JSON 'auto' 
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

user_table_insert = ("""
INSERT INTO dim_user (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT(userId)    AS user_id,
            firstName           AS first_name,
            lastName            AS last_name,
            gender,
            level
    FROM stg_event
    WHERE user_id IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO dim_song(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id as song_id,
                title as title,
                artist_id as artist_id,
                year as year,
                duration as duration
FROM stg_song
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
 INSERT INTO dim_artist(artist_id, name, location, latitude, longitude)
 SELECT  DISTINCT(artist_id) AS artist_id,
            artist_name         AS name,
            artist_location     AS location,
            artist_latitude     AS latitude,
            artist_longitude    AS longitude
    FROM stg_song
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO dim_time(start_time, hour, day, week, month, year, weekday)
SELECT distinct ts,
                EXTRACT(hour from ts),
                EXTRACT(day from ts),
                EXTRACT(week from ts),
                EXTRACT(month from ts),
                EXTRACT(year from ts),
                EXTRACT(weekday from ts)
FROM stg_event
WHERE ts IS NOT NULL;
""")

songplay_table_insert = ("""
INSERT INTO fact_songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT (se.ts) as start_time,
                se.userId as user_id,
                se.level as level,
                ss.song_id as song_id,
                ss.artist_id as artist_id,
                se.sessionId as session_id,
                se.location as location,
                se.userAgent as user_agent
FROM stg_event se
JOIN stg_song ss ON se.song = ss.title AND se.artist = ss.artist_name;
""")

#QUERY LISTS

create_table_queries = [staging_event_table_create, staging_song_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_event_table_drop, staging_song_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop]
copy_table_queries = [staging_event_copy, staging_song_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]




