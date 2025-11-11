import pandas as pd
import mysql.connector


def create_table(table_name, id, key, df, cursor, mysql_types):

    cols = ", ".join(
        f"`{col}` {mysql_types[str(dtype)]}"
        for col, dtype in zip(df.columns, df.dtypes)
    )

    cursor.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} ({id}, {cols}, {key})")


def insert_into(table, df, cursor, conn):

    cols = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))

    query = f"INSERT INTO {table}({cols}) VALUES ({placeholders})"
    values = [tuple(None if pd.isna(x) else x for x in row)
              for row in df.to_numpy()]

    cursor.executemany(query, values)
    conn.commit()


def get_id(id_col, table, column, value, cursor):

    if pd.isna(value):
        return None

    query = f"SELECT {id_col} FROM {table} WHERE {column} = %s"
    cursor.execute(query, (value,))
    result = cursor.fetchone()

    cursor.fetchall()

    return result[0] if result else None


def main():

    mysql_types = {
        "object": "VARCHAR(255)",
        "float64": "FLOAT",
        "int64": "INT",
        "bool": "TINYINT(1)"
    }

    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="ufc_db"
    )
    cursor = conn.cursor()

    # Events Table
    events = pd.read_csv("ufc_events_29_10_2025.csv")
    events_table = 'events'
    event_id = "event_id INT AUTO_INCREMENT"
    event_key = "PRIMARY KEY (event_id)"

    create_table(events_table, event_id, event_key,
                 events, cursor, mysql_types)
    insert_into(events_table, events, cursor, conn)

    # Fighters Table
    fighters = pd.read_csv("ufc_fighters_profiles_29_10_2025.csv")
    fighters_table = 'fighters'
    fighter_id = "fighter_id INT AUTO_INCREMENT"
    fighter_key = "PRIMARY KEY(fighter_id)"

    create_table(fighters_table, fighter_id, fighter_key,
                 fighters, cursor, mysql_types)
    insert_into(fighters_table, fighters, cursor, conn)


if __name__ == "__main__":
    main()
