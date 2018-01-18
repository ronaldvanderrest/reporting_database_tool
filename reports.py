#!/usr/bin/env python3
import psycopg2


def get_popular_articles():
    """Return all views per article, most views first."""
    pg = psycopg2.connect("dbname=news")
    c = pg.cursor()
    c.execute(
        "select articles.title, count(articles.title) as views "
        "from articles, log "
        "where concat('/article/', articles.slug)=log.path "
        "and log.status='200 OK' "
        "group by articles.title order by views desc "
        "limit 3;")
    return c.fetchall()
    pg.close()


def get_popular_authors():
    """Return the most popular article authors of all time,
    most views first."""
    pg = psycopg2.connect("dbname=news")
    c = pg.cursor()
    c.execute(
        "select authors.name, count(authors.name) as views "
        "from articles, log, authors "
        "where concat('/article/', articles.slug)=log.path "
        "and articles.author = authors.id and log.status='200 OK' "
        "group by authors.name order by views desc")
    return c.fetchall()
    pg.close()


def get_error_percentages():
    """Return the days that had more than 1% of the requests lead to errors."""
    pg = psycopg2.connect("dbname=news")
    c = pg.cursor()
    c.execute(
        "select date, error_percentage from (select date(time) as date, "
        "round(100.0*sum(case log.status when '200 OK'  then 0 else 1 end)/"
        "count(log.status),2) as error_percentage "
        "from log "
        "group by date order by error_percentage desc) subquery1 "
        "where error_percentage > 1;")
    return c.fetchall()
    pg.close()

if __name__ == '__main__':
    result_popular_articles = get_popular_articles()
    result_popular_authors = get_popular_authors()
    result_error_percentages = get_error_percentages()

    print "What are the most popular three articles of all time?:"
    for row in result_popular_articles:
        print "  ", row[0], " - ", row[1]

    print

    print "Who are the most popular article authors of all time?"
    for row in result_popular_authors:
        print "  ", row[0], " - ", row[1]

    print

    print "On which days did more than 1% of requests lead to errors?"
    for row in result_error_percentages:
        print "  ", row[0], " - ", row[1]
