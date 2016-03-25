__author__ = 'tracy'

import json
import psycopg2, psycopg2.extras


DB_DSN = "host=msan-beauty.cixuptbrrvfy.us-west-2.rds.amazonaws.com dbname=Beauty user=tracy password=lcy18611717064"


def transform_data(INPUT_DATA):
    """
    transform the meta data into list of tuples and
    fill in the empty cell with None
    :return:
    """
    try:
        data = list()
        f = open(INPUT_DATA)
        for line in f:
            line = json.loads(line)
            asin = line.get('asin')
            brand = line.get('brand')
            title = line.get('title')
            price = line.get('price')
            imUrl = line.get('imUrl')
            salesRank = line.get('salesRank')
            categories = line.get('categories')
            categories = sum(categories,[]) # unlist the nested list like ['...','...']
            related = line.get('related')
            data.append((asin, brand, title, price, imUrl, json.dumps(salesRank), categories, json.dumps(related)))
        f.close()

    except Exception as e:
        print e

    return data


def transform_reviews_data(INPUT_DATA):
    """
    transform the reviews data into list of tuples and
    fill in the empty cell with None
    :return:
    """
    try:
        data = list()
        f = open(INPUT_DATA)
        for line in f:
            line = json.loads(line)
            reviewerID = line.get('reviewerID')
            asin = line.get('asin')
            reviewerName = line.get('reviewerName')
            helpful = line.get('helpful')
            reviewText = line.get('reviewText')
            overall = line.get('overall')
            summary = line.get('summary')
            unixReviewTime = line.get('unixReviewTime')
            reviewTime = line.get('reviewTime')
            data.append((reviewerID, asin, reviewerName, helpful, reviewText, overall, summary, unixReviewTime,
                        reviewTime))
        f.close()

    except Exception as e:
        print e

    return data

def drop_table_beauty():
    """
    drop the table beauty if exists
    :return:
    """
    try:
        con = psycopg2.connect(dsn=DB_DSN)
        cur = con.cursor()
        cur.execute("DROP TABLE IF EXISTS beauty;")
        con.commit()

    except psycopg2.DatabaseError, e:
        print e

    cur.close()
    con.close()

def drop_table_reviews_beauty():
    """
    drop the table reviews_beauty if exists
    :return:
    """
    try:
        con = psycopg2.connect(dsn=DB_DSN)
        cur = con.cursor()
        cur.execute("DROP TABLE IF EXISTS reviews_beauty;")
        con.commit()

    except psycopg2.DatabaseError, e:
        print e

    cur.close()
    con.close()


def create_table_beauty():
    """
    :return:
    """
    try:
        con = psycopg2.connect(dsn=DB_DSN)
        cur = con.cursor()
        cur.execute("""
        CREATE TABLE beauty (
        asin TEXT UNIQUE,
        brand TEXT,
        title TEXT,
        price FLOAT,
        imUrl TEXT,
        salesRank JSON,
        categories text[],
        related JSON);
        """)
        con.commit()

    except psycopg2.DatabaseError, e:
        print e

    cur.close()
    con.close()

def create_table_reviews_beauty():
    """
    :return:
    """
    try:
        con = psycopg2.connect(dsn=DB_DSN)
        cur = con.cursor()
        cur.execute("""
        CREATE TABLE reviews_beauty (
        reviewerID TEXT,
        asin TEXT,
        reviewerName TEXT,
        helpful TEXT[],
        reviewText TEXT,
        overall FLOAT,
        summary TEXT,
        unixReviewTime INT,
        reviewTime Date);
        """)
        con.commit()

    except psycopg2.DatabaseError, e:
        print e

    cur.close()
    con.close()


def insert_data_beauty(data):
    """
    inserts the data using execute many
    :param data: a list of tuples with order (asin, brand, title, price, imUrl, salesRank, categories, related)
    :return:
    """
    try:
        con = psycopg2.connect(dsn=DB_DSN)
        cur = con.cursor()
        cur.executemany("""INSERT INTO beauty (asin, brand, title, price, imUrl, salesRank, categories, related)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);""", data)
        con.commit()

    except psycopg2.DatabaseError, e:
        print e

    cur.close()
    con.close()


def insert_data_reviews_beauty(data):
    """
    inserts the data using execute many
    :param data: a list of tuples with order (reviewerID, asin, reviewerName, helpful, reviewText,
    overall, summary, unixReviewTime, reviewTime)
    :return:
    """
    try:
        con = psycopg2.connect(dsn=DB_DSN)
        cur = con.cursor()
        cur.executemany("""INSERT INTO reviews_beauty (reviewerID, asin, reviewerName, helpful, reviewText,
                                                overall, summary, unixReviewTime, reviewTime)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);""", data)
        con.commit()

    except psycopg2.DatabaseError, e:
        print e

    cur.close()
    con.close()





