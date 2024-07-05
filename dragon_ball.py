import sqlite3
from sqlite3 import Error

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"Connected to {db_file}, sqlite version: {sqlite3.version}")
        return conn
    except Error as e:
        print(e)
    return conn

def create_tables(conn):
    try:
        with conn:
            cur = conn.cursor()
            
            create_series_sql = """
            CREATE TABLE IF NOT EXISTS series (
              id INTEGER PRIMARY KEY,
              name TEXT NOT NULL,
              episodes INTEGER NOT NULL,
              start_date TEXT,
              end_date TEXT
            );
            """
            
            create_characters_sql = """
            CREATE TABLE IF NOT EXISTS characters (
              id INTEGER PRIMARY KEY,
              series_id INTEGER NOT NULL,
              name TEXT NOT NULL,
              first_episode INTEGER NOT NULL,
              episode_count INTEGER NOT NULL,
              died TEXT NOT NULL CHECK (died IN ('Yes', 'No')),
              FOREIGN KEY (series_id) REFERENCES series (id)
            );
            """
            
            cur.execute(create_series_sql)
            cur.execute(create_characters_sql)
            
            print("Tables 'series' and 'characters' have been created successfully.")
    except Error as e:
        print(e)

def add_series(conn, series):
    sql = '''INSERT INTO series(name, episodes, start_date, end_date)
              VALUES(?,?,?,?)'''
    cur = conn.cursor()
    cur.execute(sql, series)
    conn.commit()
    return cur.lastrowid

def add_character(conn, character):
    sql = '''INSERT INTO characters(series_id, name, first_episode, episode_count, died)
              VALUES(?,?,?,?,?)'''
    cur = conn.cursor()
    cur.execute(sql, character)
    conn.commit()
    return cur.lastrowid

def update_record(conn, table, record_id, **kwargs):
    fields = ', '.join([f"{k} = ?" for k in kwargs.keys()])
    values = list(kwargs.values())
    values.append(record_id)

    sql = f"UPDATE {table} SET {fields} WHERE id = ?"

    try:
        cur = conn.cursor()
        cur.execute(sql, tuple(values))
        conn.commit()
        print(f"Record in table '{table}' with id {record_id} has been updated.")
    except Error as e:
        print(e)

def delete_record(conn, table, record_id):
    sql = f"DELETE FROM {table} WHERE id = ?"
    try:
        cur = conn.cursor()
        cur.execute(sql, (record_id,))
        conn.commit()
        print(f"Record in table '{table}' with id {record_id} has been deleted.")
    except Error as e:
        print(e)

def fetch_all_series(conn):
    cur = conn.cursor()
    cur.execute("SELECT * FROM series")
    rows = cur.fetchall()
    return rows

def fetch_all_characters(conn):
    cur = conn.cursor()
    cur.execute("SELECT * FROM characters")
    rows = cur.fetchall()
    return rows

if __name__ == "__main__":
    database = "dragon_ball.db"

    conn = create_connection(database)
    if conn is not None:
        create_tables(conn)

        series = [
            ("Dragon Ball", 153, "1986-02-26", "1989-04-19"),
            ("Dragon Ball Z", 291, "1989-04-26", "1996-01-31"),
            ("Dragon Ball GT", 64, "1996-02-07", "1997-11-19"),
            ("Dragon Ball Super", 131, "2015-07-05", "2018-03-25")
        ]
        
        for s in series:
            series_id = add_series(conn, s)
            print(f"Inserted series with ID: {series_id}")

        characters = [
            (1, "Goku", 1, 153, "No"),
            (2, "Goku", 1, 291, "Yes"),
            (3, "Goku", 1, 64, "No"),
            (4, "Goku", 1, 131, "No"),
            (2, "Vegeta", 6, 247, "Yes"),
            (3, "Vegeta", 1, 64, "No"),
            (4, "Vegeta", 6, 116, "No"),
            (1, "Piccolo", 23, 78, "No"),
            (2, "Piccolo", 1, 155, "Yes"),
            (3, "Piccolo", 1, 64, "No"),
            (4, "Piccolo", 1, 131, "No"),
            (1, "Kuririn", 14, 136, "Yes"),
            (2, "Kuririn", 1, 194, "No"),
            (3, "Kuririn", 1, 64, "Yes"),
            (4, "Kuririn", 1, 69, "No"),
            (2, "Gohan", 1, 195, "Yes"),
            (3, "Gohan", 1, 64, "No"),
            (4, "Gohan", 1, 80, "No"),
            (2, "Trunks", 107, 84, "Yes"),
            (3, "Trunks", 1, 64, "No"),
            (4, "Trunks", 1, 78, "No"),
            (2, "Goten", 180, 61, "Yes"),
            (3, "Goten", 1, 64, "No"),
            (4, "Goten", 1, 26, "No"),
            (2, "Frieza", 44, 33, "Yes"),
            (4, "Frieza", 19, 57, "No"),
            (1, "Tien", 14, 137, "Yes"),
            (2, "Tien", 1, 68, "Yes"),
            (3, "Tien", 1, 64, "No"),
            (4, "Tien", 1, 29, "No"),
            (1, "Yamcha", 5, 96, "Yes"),
            (2, "Yamcha", 1, 40, "Yes"),
            (3, "Yamcha", 1, 64, "No"),
            (4, "Yamcha", 1, 19, "No")
        ]
        
        for c in characters:
            character_id = add_character(conn, c)
            print(f"Inserted character with ID: {character_id}")

        series_rows = fetch_all_series(conn)
        print("Series:")
        for row in series_rows:
            print(row)

        characters_rows = fetch_all_characters(conn)
        print("Characters:")
        for row in characters_rows:
            print(row)

        update_record(conn, 'characters', 1, episode_count=154)
        delete_record(conn, 'characters', 7)

        characters_rows = fetch_all_characters(conn)
        print("Characters after update and deletion:")
        for row in characters_rows:
            print(row)

        conn.close()