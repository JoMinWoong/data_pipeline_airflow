class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
    
    users_table_upsert_delete = ("""
        DELETE FROM users
        USING  staging_events
        WHERE  users.userid = staging_events.userid 
    """)
    
    songs_table_upsert_delete = ("""
        DELETE FROM songs
        USING  staging_songs
        WHERE  songs.songid = staging_songs.song_id
    """)
    
    artists_table_upsert_delete = ("""
        DELETE FROM artists
        USING  staging_songs
        WHERE  artists.artistid = staging_songs.artist_id
    """)
    
#     check_queries = [
#         { 'check_sql': 'SELECT COUNT(*) FROM public.songplays WHERE userid IS NULL OR playid  IS NULL', 'expected_result': 0 }, 
#         { 'check_sql': 'SELECT COUNT(DISTINCT "level") FROM public.songplays', 'expected_result': 2 },
#         { 'check_sql': 'SELECT COUNT(*) FROM public.artists WHERE name IS NULL', 'expected_result': 0 },
#         { 'check_sql': 'SELECT COUNT(*) FROM public.songs WHERE title IS NULL', 'expected_result': 0 },
#         { 'check_sql': 'SELECT COUNT(*) FROM public.users WHERE first_name IS NULL', 'expected_result': 0 },
#         { 'check_sql': 'SELECT COUNT(*) FROM public."time" WHERE weekday IS NULL OR hour > 24 OR day > 31 OR month > 12', 'expected_result': 0 },
#         { 'check_sql': 'SELECT COUNT(*) FROM public.songplays sp LEFT OUTER JOIN public.users u ON u.userid = sp.userid WHERE u.userid IS NULL', \
#          'expected_result': 0 }
#     ]
    
    check_queries_songplays = [
        { 'check_sql': 'SELECT COUNT(*) FROM public.songplays WHERE userid IS NULL OR playid  IS NULL', 'expected_result': 0 },
        { 'check_sql': 'SELECT COUNT(DISTINCT "level") FROM public.songplays', 'expected_result': 2 }
    ]

    check_queries_artists = [
        { 'check_sql': 'SELECT COUNT(*) FROM public.artists WHERE name IS NULL', 'expected_result': 0 }
    ]
    
    check_queries_songs = [
        { 'check_sql': 'SELECT COUNT(*) FROM public.songs WHERE title IS NULL', 'expected_result': 0 }
    ]
    
    check_queries_users = [
        { 'check_sql': 'SELECT COUNT(*) FROM public.users WHERE first_name IS NULL', 'expected_result': 0 }
    ]
    
    check_queries_time = [
        { 'check_sql': 'SELECT COUNT(*) FROM public."time" WHERE weekday IS NULL OR hour > 24 OR day > 31 OR month > 12', 'expected_result': 0 },
    ]