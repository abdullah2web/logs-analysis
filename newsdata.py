# import postgreSQL Database.
import psycopg2

# select first query and put in function.
def pupular_artical():
    cursor.execute("select title, count(*) as views from articles join\
    log on concat('/article/', articles.slug) = log.path\
    where log.status like '%200%'\
    group by log.path, articles.title order by views desc limit 3")
    results = cursor.fetchall()
    return results

# select second query and put in function.
def pupluar_author():
    cursor.execute("select authors.name, count(*) as views from articles join\
    authors on articles.author = authors.id join\
    log on concat('/article/', articles.slug) = log.path where\
    log.status like '%200%' group by authors.name order by views desc")
    results = cursor.fetchall()
    return results

# select third query and put in function.
def errors_days():
    cursor.execute("select * from (\
    select a.day,\
    round(cast((100*b.hits) as numeric) / cast(a.hits as numeric), 2)\
    as perc from\
    (select date(time) as day, count(*) as hits from log group by day) as a\
    join\
    (select date(time) as day, count(*) as hits from log where status\
    like '%404%' group by day) as b\
    on a.day = b.day)\
    as t where perc >= 1.0")
    results = cursor.fetchall()
    return results

# arrange first 2 queries by loop and put in function.
def print_query(query):
    for index in range(len(query)):
        a = query[index][0]
        res = query[index][1]
        print("\t" + "%s - %d" % (a, res) + " views")
    print("\n")

# arrange the last query by loop and put in function.
def print_last_query(query_result):
    for index in range(len(query_result)):
        d = query_result[index][0]
        perc = query_result[index][1]
        print("\t" + "%s - %.1f %%" % (d, perc))

# print and view all queries arrangement.
dbname = 'news'
db = psycopg2.connect("dbname=news")
cursor = db.cursor()
print("The three pupluar articles:")
result = pupular_artical()
print_query(result)
print("The pupluar authors:")
result = pupluar_author()
print_query(result)
print("Days more than 1% of requests lead to errors:")
result = errors_days()
print_last_query(result)
db.close()
