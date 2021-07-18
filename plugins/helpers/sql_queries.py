class InsertQueries:
    songplays_table_incremental_load = """
    INSERT INTO {target_table} 
    (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    (
        WITH temp1 AS (
            SELECT se.*, s.song_id, a.artist_id 
            FROM staging_events se
            JOIN songs s ON s.title = se.song
            JOIN artists a ON a.artist_id = s.artist_id 
            WHERE CAST(se.length AS INT) = CAST(s.duration AS INT)
        )
        SELECT 
            date_add('ms', se.ts, '1970-01-01') as start_time
            ,se.user_id
            ,se.level
            ,t1.song_id
            ,t1.artist_id
            ,se.session_id
            ,se.location
            ,se.user_agent
            
        FROM {source_table} se
        LEFT JOIN temp1 t1 ON t1.staging_events_id = se.staging_events_id 
        WHERE se.page = 'NextSong'
        AND date_add('ms', se.ts, '1970-01-01') > (SELECT COALESCE(MAX(start_time), '1800-01-01') FROM {target_table})
    );
    """

    users_table_full_load = """
    INSERT INTO {target_table}
    (user_id, first_name, last_name, gender, level)
    (
        WITH temp_users AS (
            SELECT se.*, ROW_NUMBER() OVER (PARTITION BY se.user_id ORDER BY se.ts DESC) as seqnum
            FROM {source_table} se)
        SELECT DISTINCT 
            tu.user_id,
            tu.first_name,
            tu.last_name,
            tu.gender,
            tu.level
        FROM temp_users tu
        WHERE 
            tu.seqnum = 1 
            AND tu.page = 'NextSong'
            AND tu.user_id IS NOT NULL
    );
    """

    users_table_incremental_load = """
    INSERT INTO {target_table}
    (user_id, first_name, last_name, gender, level)
    (
        WITH temp_users AS (
            SELECT se.*, ROW_NUMBER() OVER (PARTITION BY se.user_id ORDER BY se.ts DESC) as seqnum
            FROM {source_table} se)
        SELECT DISTINCT 
            tu.user_id,
            tu.first_name,
            tu.last_name,
            tu.gender,
            tu.level
        FROM temp_users tu
        WHERE 
            tu.seqnum = 1 
            AND tu.page = 'NextSong'
            AND tu.user_id IS NOT NULL
            AND tu.user_id NOT IN (SELECT user_id FROM {target_table})
    );
    """

    songs_table_full_load = """
    INSERT INTO {target_table}
    (song_id, title, artist_id, year, duration)
    (
        WITH temp_songs AS (
            SELECT ss.*, ROW_NUMBER() OVER (PARTITION BY ss.song_id ORDER BY ss.year) as seqnum
            FROM {source_table} ss)
        SELECT DISTINCT 
            ts.song_id,
            ts.title,
            ts.artist_id,
            ts.year,
            ts.duration
        FROM temp_songs ts
        WHERE ts.seqnum = 1 
    );
    """

    songs_table_incremental_load = """
    INSERT INTO {target_table}
    (song_id, title, artist_id, year, duration)
    (
        WITH temp_songs AS (
            SELECT ss.*, ROW_NUMBER() OVER (PARTITION BY ss.song_id ORDER BY ss.year) as seqnum
            FROM {source_table} ss)
        SELECT DISTINCT 
            ts.song_id,
            ts.title,
            ts.artist_id,
            ts.year,
            ts.duration
        FROM temp_songs ts
        WHERE ts.seqnum = 1 
        AND ts.song_id NOT IN (SELECT song_id FROM {target_table})
    );
    """

    artists_table_full_load = """
    INSERT INTO {target_table}
    (artist_id, name, location, latitude, longitude)
    (
        WITH temp_songs AS (
            SELECT ss.*, ROW_NUMBER() OVER (PARTITION BY ss.artist_id ORDER BY ss.year) as seqnum
            FROM {source_table} ss)
        SELECT DISTINCT 
            ts.artist_id,
            ts.artist_name,
            ts.artist_location,
            ts.artist_latitude,
            ts.artist_longitude
        FROM temp_songs ts
        WHERE ts.seqnum = 1 
    );
    """

    artists_table_incremental_load = """
    INSERT INTO {target_table}
    (artist_id, name, location, latitude, longitude)
    (
        WITH temp_songs AS (
            SELECT ss.*, ROW_NUMBER() OVER (PARTITION BY ss.artist_id ORDER BY ss.year) as seqnum
            FROM {source_table} ss)
        SELECT DISTINCT 
            ts.artist_id,
            ts.artist_name,
            ts.artist_location,
            ts.artist_latitude,
            ts.artist_longitude
        FROM temp_songs ts
        WHERE ts.seqnum = 1
        AND ts.artist_id NOT IN (SELECT artist_id FROM {target_table}) 
    );
    """

    time_table_full_load = """
        INSERT INTO {target_table}
        (start_time, hour, day, week, month, year, weekday)
        (
            SELECT DISTINCT date_add('ms', se.ts, '1970-01-01') as start_time 
                ,EXTRACT(HOUR FROM date_add('ms', se.ts, '1970-01-01')) as hour
                ,EXTRACT(DAY FROM date_add('ms', se.ts, '1970-01-01')) as day
                ,EXTRACT(WEEK FROM date_add('ms', se.ts, '1970-01-01')) as week
                ,EXTRACT(MONTH FROM date_add('ms', se.ts, '1970-01-01')) as month
                ,EXTRACT(YEAR FROM date_add('ms', se.ts, '1970-01-01')) as year
                ,EXTRACT (WEEKDAY FROM date_add('ms', se.ts, '1970-01-01')) as weekday
            FROM {source_table} se
        )
    """

    time_table_incremental_load = """
        INSERT INTO {target_table}
        (start_time, hour, day, week, month, year, weekday)
        (
            SELECT DISTINCT date_add('ms', se.ts, '1970-01-01') as start_time 
                ,EXTRACT(HOUR FROM date_add('ms', se.ts, '1970-01-01')) as hour
                ,EXTRACT(DAY FROM date_add('ms', se.ts, '1970-01-01')) as day
                ,EXTRACT(WEEK FROM date_add('ms', se.ts, '1970-01-01')) as week
                ,EXTRACT(MONTH FROM date_add('ms', se.ts, '1970-01-01')) as month
                ,EXTRACT(YEAR FROM date_add('ms', se.ts, '1970-01-01')) as year
                ,EXTRACT (WEEKDAY FROM date_add('ms', se.ts, '1970-01-01')) as weekday
            FROM {source_table} se
            AND {target_table}.start_time NOT IN (SELECT date_add('ms', ts, '1970-01-01') FROM {source_table})
        )
    """


class CreateQueries:
    staging_events_table_create = """
        CREATE TABLE IF NOT EXISTS staging_events(
            staging_events_id BIGINT IDENTITY(1,1),
            artist VARCHAR(255),
            auth VARCHAR(50),
            first_name VARCHAR(255),
            gender VARCHAR(10),
            item_in_session INT,
            last_name VARCHAR(255),
            length FLOAT,
            level VARCHAR(20),
            location VARCHAR(255),
            method VARCHAR(20),
            page VARCHAR(30),
            registration BIGINT,
            session_id BIGINT,
            song VARCHAR(255),
            status INT,
            ts BIGINT,
            user_agent VARCHAR(255),
            user_id INT,
            PRIMARY KEY(staging_events_id)
        );
    """

    staging_songs_table_create = """
        CREATE TABLE IF NOT EXISTS staging_songs(
            staging_songs_id BIGINT IDENTITY(1,1),
            num_songs INT,
            artist_id VARCHAR(50),
            artist_latitude FLOAT,
            artist_longitude FLOAT,
            artist_location VARCHAR(255),
            artist_name VARCHAR(255),
            song_id VARCHAR(50),
            title VARCHAR(255),
            duration FLOAT,
            year INT,
            PRIMARY KEY(staging_songs_id)
        );
    """

    songplays_table_create = """
        CREATE TABLE IF NOT EXISTS songplays(
            songplay_id BIGINT IDENTITY(1,1), 
            start_time TIMESTAMP, 
            user_id INT NOT NULL, 
            level VARCHAR, 
            song_id VARCHAR(100),
            artist_id VARCHAR(100),
            session_id INT,
            location VARCHAR,
            user_agent VARCHAR,
            PRIMARY KEY(songplay_id),
            FOREIGN KEY(user_id) REFERENCES users(user_id),
            FOREIGN KEY(song_id) REFERENCES songs(song_id),
            FOREIGN KEY(artist_id) REFERENCES artists(artist_id)
        );
    """

    users_table_create = """
        CREATE TABLE IF NOT EXISTS users(
            user_id INT NOT NULL, 
            first_name VARCHAR, 
            last_name VARCHAR, 
            gender VARCHAR, 
            level VARCHAR,
            PRIMARY KEY(user_id)
        );
    """

    songs_table_create = """
        CREATE TABLE IF NOT EXISTS songs(
            song_id VARCHAR(100) NOT NULL, 
            title VARCHAR(255), 
            artist_id VARCHAR(100) NOT NULL, 
            year INT, 
            duration int, 
            PRIMARY KEY(song_id),
            FOREIGN KEY(artist_id) REFERENCES artists(artist_id)
        );
    """

    artists_table_create = """
        CREATE TABLE IF NOT EXISTS artists(
            artist_id VARCHAR(100), 
            name VARCHAR(255), 
            location VARCHAR(255), 
            latitude FLOAT, 
            longitude FLOAT,
            PRIMARY KEY(artist_id)
        );
    """

    time_table_create = """
        CREATE TABLE IF NOT EXISTS time(
            start_time TIMESTAMP, 
            hour INT, 
            day INT, 
            week INT,
            month INT, 
            year INT, 
            weekday INT,
            PRIMARY KEY(start_time)
        );
    """


class DeleteRowsQueries:
    delete_table_rows = """
    DELETE FROM {table_name};
    """
