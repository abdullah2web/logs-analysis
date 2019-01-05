# import postgreSQL
import psycopg2

# first query (The Best Three Articles).
db1 = psycopg2.connect("dbname=news")
c = db1.cursor()

query1 = ("select title, count(*) as best\
    from articles join log\
    on concat('/article/', articles.slug) = log.path\
    where log.status like '%200%'\
    group by log.path, articles.title\
    order by best desc limit 3")

c.execute(query1)
rows = c.fetchall()
print("The Best Three Articles:")
for row in rows:
    print("  ", row[0], "-", row[1], " views")
db1.close()

print(" ")

# second query (The Top Views Authors).
db2 = psycopg2.connect("dbname=news")
c = db2.cursor()

query2 = ("select authors.name, count(*) as top\
    from articles join authors\
    on articles.author = authors.id join log\
    on concat('/article/', articles.slug) = log.path\
    where log.status like '%200%' group by authors.name\
    order by top desc")

c.execute(query2)
rows = c.fetchall()
print("The Top Views Authors:")
for row in rows:
    print("  ", row[0], "-", row[1], " views")
db2.close()

print(" ")

# third query (Day more than 1% of requests lead to errors).
db3 = psycopg2.connect("dbname=news")
c = db3.cursor()

query3 = ("select * from (select one.days,\
    round(cast((100*tow.cos) as numeric) / cast(one.cos as numeric), 2)\
    as percent from (select date(time) as days, count(*) as cos\
    from log group by days) as one join\
    (select date(time) as days, count(*) as cos from log where status\
    like '%404%' group by days) as tow\
    on one.days = tow.days) as plus\
    where percent >= 1.0")

c.execute(query3)
rows = c.fetchall()
print("Days more than 1% of requests lead to errors:")
for row in rows:
    print("  ", row[0], "-", row[1], "% errors")
db3.close()
