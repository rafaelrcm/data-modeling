Description: 
In this project I will extract data from csv files and populate the dimension and fact tables.

Fact Table:
songplays: serial, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dim Tables:
users: user_id, first_name, last_name, gender, level
songs: song_id, title, artist_id, year, duration
artists: artist_id, name, location, latitude, longitude
time: start_time, hour, day, week, month, year, weekday

Special Cases:
To handle duplicate scenarios the clause ON CONFLICT is applied to SQL Queries

Run the scripts:
- Run the create_tables.py
- Run the etl.py


