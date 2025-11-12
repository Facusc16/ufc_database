import pandas as pd
import mysql.connector


def create_table(table, df, mysql_types, cursor):

    cols = ", ".join(f"`{col}` {mysql_types[str(dtype)]}"
                     for col, dtype in zip(df.columns, df.dtypes))

    fk = f", {", ".join(f"FOREIGN KEY ({col[0]}) REFERENCES {col[1]} ({col[2]})"
                        for col in table['fk'])}" if table['fk'] else ""

    cursor.execute(
        f"""CREATE TABLE IF NOT EXISTS `{table['name']}` (
            `{table['id']}` INT AUTO_INCREMENT,
            {cols}
            {fk},
            PRIMARY KEY (`{table['pk']}`));"""
    )


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
    events_table = {
        'name': 'events',
        'id': 'event_id',
        'fk': "",
        'pk': 'event_id'
    }

    create_table(events_table, events, mysql_types, cursor)
    insert_into(events_table['name'], events, cursor, conn)

    # Fighters Table
    fighters = pd.read_csv("ufc_fighters_profiles_29_10_2025.csv")
    fighters_table = {
        'name': 'fighters',
        'id': 'fighter_id',
        'fk': '',
        'pk': 'fighter_id'
    }

    create_table(fighters_table, fighters, mysql_types, cursor)
    insert_into(fighters_table['name'], fighters, cursor, conn)


if __name__ == "__main__":
    main()
