import pandas as pd
import mysql.connector


def create_table(table_name, df, cursor, mysql_types):

    cols = ", ".join(
        f"`{col}` {mysql_types[str(dtype)]}"
        for col, dtype in zip(df.columns, df.dtypes)
    )

    cursor.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} (id int not null AUTO_INCREMENT, {cols}, PRIMARY KEY(id))")


def insert_into(df, table, cursor, conn):

    cols = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))

    query = f"INSERT INTO {table}({cols}) VALUES ({placeholders})"
    values = [tuple(None if pd.isna(x) else x for x in row)
              for row in df.to_numpy()]

    cursor.executemany(query, values)
    conn.commit()


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

    datasets = [("ufc_events_29_10_2025.csv", "events"),
                ("ufc_fighters_profiles_29_10_2025.csv", "fighters"),
                ("ufc_fights_29_10_2025.csv", "fights")]

    for dataset in datasets:

        df = pd.read_csv(dataset[0])
        create_table(dataset[1], df, cursor, mysql_types)
        insert_into(df, dataset[1], cursor, conn)


if __name__ == "__main__":
    main()
