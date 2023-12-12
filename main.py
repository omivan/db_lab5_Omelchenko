import psycopg2
import matplotlib.pyplot as plt

username = 'omivan'
password = '0104'
database = 'lab4_games'
host = 'localhost'
port = '5432'


query_1 = '''
CREATE VIEW company_game_counts AS
SELECT
  c.name AS company_name,
  COUNT(DISTINCT p.game_id) + COUNT(DISTINCT d.game_id) AS total_count
FROM
  company c
LEFT JOIN
  publish p ON c.company_id = p.company_id
LEFT JOIN
  develop d ON c.company_id = d.company_id
GROUP BY
  c.company_id, c.name
ORDER BY
  COUNT(DISTINCT p.game_id) + COUNT(DISTINCT d.game_id) DESC;
'''
query_2 = '''
CREATE VIEW genre_usage_counts AS
SELECT
  genre.name AS genre_name,
  COUNT(game_genre.genre_id) AS usage_count
FROM
  genre
LEFT JOIN
  game_genre ON genre.genre_id = game_genre.genre_id
GROUP BY
  genre.genre_id, genre.name
ORDER BY 
  COUNT(game_genre.genre_id) DESC;
'''
query_3 = '''
CREATE VIEW company_total_users AS
SELECT
  company.name,
  COALESCE(SUM(game.users_number), 0) AS total_users
FROM
  company
LEFT JOIN
  publish ON company.company_id = publish.company_id
LEFT JOIN
  game ON publish.game_id = game.game_id
GROUP BY
  company.company_id, company.name
ORDER BY
  COALESCE(SUM(game.users_number), 0) DESC;

'''

conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

with conn:
    cur = conn.cursor()
    cur.execute('DROP VIEW IF EXISTS company_game_counts')
    cur.execute(query_1)
    cur.execute('SELECT * FROM company_game_counts LIMIT 5;')
    companies = []
    total = []

    for row in cur:
        companies.append(row[0])
        total.append(row[1])

    x_range = range(len(companies))

    figure, (bar_ax, pie_ax, graph_ax) = plt.subplots(1, 3)
    bar = bar_ax.bar(x_range, total)
    bar_ax.bar_label(bar, label_type='center')
    bar_ax.set_xticks(x_range)
    bar_ax.set_xticklabels(companies, rotation=45, ha='right')
    bar_ax.set_xlabel('Компанії')
    bar_ax.set_ylabel('Кількість разів')
    bar_ax.set_title('Кількість разів компанія була згадана як publisher або developer(топ 5 DESC)')

    cur.execute('DROP VIEW IF EXISTS genre_usage_counts')
    cur.execute(query_2)
    cur.execute('SELECT * FROM genre_usage_counts LIMIT 5;')
    genres = []
    total = []

    for row in cur:
        genres.append(row[0])
        total.append(row[1])

    x_range = range(len(genres))
    pie_ax.pie(total, labels=genres, autopct='%1.1f%%')
    pie_ax.set_title('Частка кожного жанру в іграх(топ 5 DESC)')

    # cur.execute(query_3)
    cur.execute('DROP VIEW IF EXISTS company_total_users')
    cur.execute(query_3)
    cur.execute('SELECT * FROM company_total_users ORDER BY total_users DESC LIMIT 5;')
    companies = []
    users_num = []

    for row in cur:
        companies.append(row[0])
        users_num.append(row[1])

    mark_color = 'blue'
    graph_ax.plot(companies, users_num, color=mark_color, marker='o')

    for qnt, price in zip(companies, users_num):
        graph_ax.annotate(price, xy=(qnt, price), color=mark_color,
                          xytext=(7, 2), textcoords='offset points')

    graph_ax.set_xlabel('Назва компанії')
    graph_ax.set_ylabel('Кількість користувачів')
    graph_ax.set_xticklabels(companies, rotation=45, ha="right")
    graph_ax.plot(companies, users_num, color='blue', marker='o')
    graph_ax.set_title('Топ 5 компанії за кількістю користувачів')

mng = plt.get_current_fig_manager()
mng.full_screen_toggle()
# mng.resize(1400, 600)
plt.savefig('plots.png', bbox_inches='tight')
plt.show()
