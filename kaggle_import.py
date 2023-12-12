import psycopg2
import pandas as pd
import time

start_time = time.time()

def read_csv():
    data = pd.read_csv("data.csv", on_bad_lines='skip')
    data["number_users"] = data["owners"].apply(lambda x: x.split(' .. ')[1])
    data["number_users"] = data["number_users"].apply(lambda x: int(x.replace(",", "")))
    data = data.sort_values(by="number_users", ascending=False).reset_index(drop=True)

    return data


def db_connect():
    username = 'omivan'
    password = '0104'
    database = 'lab4_games'
    host = 'localhost'
    port = '5432'
    conn = psycopg2.connect(user=username, password=password,
                            dbname=database, host=host, port=port)
    return conn


def create_company_table(conn):
    with conn:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS company CASCADE;")
        cur.execute(f"CREATE TABLE company(company_id INT NOT NULL,"
                    f" location VARCHAR(50) NOT NULL,name VARCHAR(50) NOT NULL,"
                    f"PRIMARY KEY (company_id));")


def create_genre_table(conn):
    with conn:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS genre CASCADE;")
        cur.execute(f"CREATE TABLE genre(genre_id INT NOT NULL,"
                    f"name VARCHAR(50) NOT NULL,"
                    f"PRIMARY KEY (genre_id));")


def create_game_table(conn):
    with conn:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS game CASCADE;")
        cur.execute(f"CREATE TABLE game(game_id INT NOT NULL,"
                    f"name VARCHAR(50) NOT NULL,"
                    f"release_date DATE NOT NULL,"
                    f"users_number INT NOT NULL,"
                    f"PRIMARY KEY (game_id));")


def create_game_genre_table(conn):
    with conn:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS game_genre CASCADE;")
        cur.execute(f"CREATE TABLE game_genre(genre_id INT NOT NULL,"
                    f"game_id INT NOT NULL,PRIMARY KEY (genre_id, game_id),"
                    f"FOREIGN KEY (genre_id) REFERENCES genre(genre_id),"
                    f"FOREIGN KEY (game_id) REFERENCES game(game_id));")


def create_develop_publish_table(conn, name):
    with conn:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {name} CASCADE;")
        cur.execute(f"CREATE TABLE {name}(game_id INT NOT NULL,"
                    f"company_id INT NOT NULL,PRIMARY KEY (game_id, company_id),"
                    f"FOREIGN KEY (game_id) REFERENCES game(game_id),"
                    f"FOREIGN KEY (company_id) REFERENCES company(company_id));")


def insert_company(cur, company_name, location='None'):
    cur.execute("SELECT company_id FROM company WHERE name = %s", (company_name,))
    existing_company = cur.fetchone()
    if existing_company:
        return existing_company[0]
    else:
        cur.execute("SELECT MAX(company_id) FROM company")
        max_company_id = cur.fetchone()[0] or 0
        new_company_id = max_company_id + 1
        cur.execute("INSERT INTO company(company_id, location, name) VALUES (%s, %s, %s)",
                    (new_company_id, location, company_name))

        return new_company_id


def insert_genre(cur, genre_name):
    cur.execute("SELECT genre_id FROM genre WHERE name = %s", (genre_name,))
    existing_company = cur.fetchone()
    if existing_company:
        return existing_company[0]
    else:
        cur.execute("SELECT MAX(genre_id) FROM genre")
        max_genre_id = cur.fetchone()[0] or 0
        new_genre_id = max_genre_id + 1
        cur.execute("INSERT INTO genre(genre_id, name) VALUES (%s, %s)",
                    (new_genre_id, genre_name))

        return new_genre_id


def insert_game(cur, game_name, users_number, release_date='2004-04-01'):
    cur.execute("SELECT MAX(game_id) FROM game")
    max_game_id = cur.fetchone()[0] or 0
    new_game_id = max_game_id + 1
    cur.execute("INSERT INTO game(game_id, name, release_date, users_number)"
                " VALUES (%s, %s, %s, %s)",
                (new_game_id, game_name, release_date, users_number))
    return new_game_id


def link_game_genre(cur, game_id, genre_id):
    cur.execute("SELECT COUNT(*) FROM game_genre WHERE genre_id = %s AND game_id = %s",
                (genre_id, game_id))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute("INSERT INTO game_genre(genre_id, game_id) VALUES (%s, %s)",
                    (genre_id, game_id))


def link_game_publisher(cur, game_id, publisher_id):
    cur.execute("SELECT COUNT(*) FROM publish WHERE game_id = %s AND company_id = %s",
                (game_id, publisher_id))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute("INSERT INTO publish(game_id, company_id) VALUES (%s, %s)",
                    (game_id, publisher_id))


def link_game_developer(cur, game_id, developer_id):
    cur.execute("SELECT COUNT(*) FROM develop WHERE game_id = %s AND company_id = %s",
                (game_id, developer_id))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute("INSERT INTO develop(game_id, company_id) VALUES (%s, %s)",
                    (game_id, developer_id))


if __name__ == "__main__":
    data = read_csv()
    conn = db_connect()
    create_company_table(conn)
    create_genre_table(conn)
    create_game_table(conn)
    create_game_genre_table(conn)
    create_develop_publish_table(conn, "publish")
    create_develop_publish_table(conn, "develop")
    for index, row in data.iterrows():
        if index == 10001:
            break
        with conn:
            cur = conn.cursor()
        if int(index) % 1000 == 0:
            elapsed_time = time.time() - start_time
            print(f"Imported {index}, Elapsed Time: {round(elapsed_time, 2)} seconds")
            start_time = time.time()
        game_name = row["name"].strip()[:49]
        user_number = row["number_users"]
        genres = [x.strip()[:49] for x in str(row["genre"]).split(',')]
        developers = []
        publishers = []
        for x in str(row["developer"]).split(','):
            formatted_name = str(x).strip()[:49]
            if formatted_name not in ["Inc.", "Inc.", "LLC", "Ltd."]:
                developers.append(formatted_name)
        for x in str(row["publisher"]).split(','):
            formatted_name = str(x).strip()[:49]
            if formatted_name not in ["Inc.", "Inc.", "LLC", "Ltd."]:
                publishers.append(formatted_name)
        # developers = [str(x).strip()[:49] for x in str(row["developer"]).split(',')]
        # publishers = [str(x).strip()[:49] for x in str(row["publisher"]).split(',')]
        developers_ids = [insert_company(cur, x) for x in developers]
        publishers_ids = [insert_company(cur, x) for x in publishers]
        genres_ids = [insert_genre(cur, x) for x in genres]
        game_id = insert_game(cur, game_name, user_number)
        for x in genres_ids:
            link_game_genre(cur, game_id, x)
        for x in publishers_ids:
            link_game_publisher(cur, game_id, x)
        for x in developers_ids:
            link_game_developer(cur, game_id, x)

    # gnr = (insert_genre(conn, "A"))
    #
    # gm = (insert_game(conn, "test_game", 100))
    # gm2 = (insert_game(conn, "test_game1", 1000))
    # link_game_genre(gm, gnr)
    # link_game_genre(gm2, gnr)
    # cmp = insert_company(conn, "tete")
    # link_game_publisher(gm, cmp)
    # link_game_developer(gm, cmp)
