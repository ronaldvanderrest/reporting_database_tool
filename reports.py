#!/usr/bin/env python3
import psycopg2


def get_popular_articles():
    """Return all views per article, most views first."""
    pg = psycopg2.connect("dbname=news")
    c = pg.cursor()
    c.execute(
        "SELECT articles.title, count(articles.title) AS views "
        "FROM articles, log "
        "WHERE concat('/article/', articles.slug)=log.path "
        "AND log.status='200 OK' "
        "GROUP BY articles.title ORDER BY views DESC "
        "LIMIT 3;")
    return c.fetchall()
    pg.close()


def get_popular_authors():
    """Return the most popular article authors of all time,
    most views first."""
    pg = psycopg2.connect("dbname=news")
    c = pg.cursor()
    c.execute(
        "SELECT authors.name, count(authors.name) AS views "
        "FROM articles, log, authors "
        "WHERE concat('/article/', articles.slug)=log.path "
        "AND articles.author = authors.id AND log.status='200 OK' "
        "GROUP BY authors.name ORDER BY views DESC")
    return c.fetchall()
    pg.close()


def get_error_percentages():
    """Return the days that had more than 1% of the requests lead to errors."""
    pg = psycopg2.connect("dbname=news")
    c = pg.cursor()
    c.execute(
        "SELECT date, error_percentage FROM (select date(time) AS date, "
        "round(100.0*sum(case log.status when '200 OK'  then 0 else 1 end)/"
        "count(log.status),2) AS error_percentage "
        "FROM log "
        "GROUP BY date ORDER BY error_percentage desc) subquery1 "
        "WHERE error_percentage > 1;")
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
