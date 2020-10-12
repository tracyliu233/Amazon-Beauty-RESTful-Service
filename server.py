from flask import Flask, request, jsonify
import psycopg2, psycopg2.extras
import collections

# DSN location of the AWS - RDS instance

app = Flask(__name__)


@app.route('/')
def default():

    output = dict()
    output['message'] = "Welcome to Tracy's app!"

    return jsonify(output)


@app.route('/reviews/reviewername/counts')
def get_counts_per_reviewername():
    """
    calculates the total number of each reviewername
    this function only show 10 rows as example
    :return: a dict of key = reviewername and value = count
    """

    out = dict()

    sql = """
    SELECT reviewername, COUNT(reviewername) num FROM reviews_beauty
    group by reviewername
    order by num DESC
    limit 10;
    """
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs = cur.fetchall()
        for item in rs:
            out[item['reviewername']] = item['num']
    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

    return jsonify(out)

@app.route('/reviews/reviewername/counts/<reviewername>')
def get_counts_by_reviewername(reviewername):
    """
    calculates the total number of reviews by a reviewername
    :param reviewername
    :return: a dict of key = reviewername and value = count
    """

    out = dict()

    sql = "SELECT COUNT(*) num FROM reviews_beauty WHERE reviewername = %s;"
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, (reviewername, ))
        rs = cur.fetchone()
        out[reviewername] = rs['num']
    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

    return jsonify(out)

@app.route('/reviews/overall/avg_overall')
def get_avg_overall_per_asin():
    """
    calculates the average overall rating for each asin
    this function only show 10 rows as example
    :return: a dict of key = asin and value = average overall rating
    """

    out = dict()

    sql = "select asin, avg(overall) avg_overall from reviews_beauty group by asin limit 10;"
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs = cur.fetchall()
        for item in rs:
            out[item['asin']] = item['avg_overall']
    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

    return jsonify(out)

@app.route('/reviews/overall/avg_overall/<asin>')
def get_avg_overall_by_asin(asin):
    """
    calculates the average overall rating for an asin
    this function only show 10 rows as example
    :param asin
    :return: a dict of key = asin and value = average overall rating
    """

    out = dict()
    sql = "select avg(overall) avg_overall from reviews_beauty where asin = %s;"

    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, (asin, ))
        rs = cur.fetchone()
        out[asin] = rs['avg_overall']
    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

    return jsonify(out)

@app.route('/reviews/helpful/fraction')
def get_helpful_per_user():
    """
    calculate the number of helpfuls and total reviews for each reviewername
    this function only show 10 rows as example
    :return: a dict of key = reviewername and value = [total number of helpful, total number of reviews]
    """

    out = collections.defaultdict(list)

    sql = """
    select reviewername,
    sum(helpful[1]::int) helpful,
    sum(helpful[2]::int) all_comments
    from reviews_beauty
    where reviewername != 'null'
    group by reviewername
    order by helpful DESC
    limit 10;
    """

    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs = cur.fetchall()
        for item in rs:
            out[item['reviewername']].append(item['helpful'])
            out[item['reviewername']].append(item['all_comments'])
    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

    return jsonify(out)

@app.route('/meta/title/avg_price')
def get_avg_price_per_title():
    """
    Calculate the average price for each name of the product
    this function only show 10 rows as example
    :return: a dict of key = title and value = average price
    """

    out = dict()
    sql = "select title, avg(price) avg_price from beauty group by title order by avg_price ASC limit 10"

    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs = cur.fetchall()
        for item in rs:
            out[item['title']] = item['avg_price']
    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

    return jsonify(out)

@app.route('/meta/title/avg_price/<title>')
def get_avg_price_by_title(title):
    """
    Calculate the average price of one title
    :param title
    :return: a dict of key = title and value = average price
    """
    out = dict()
    sql = "select avg(price) avg_price from beauty where title = %s;"
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, (title, ))
        rs = cur.fetchone()
        out[title] = rs['avg_price']
    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

    return jsonify(out)


if __name__ == "__main__":
    # app.debug = True # only have this on for debugging!
    app.run(host='0.0.0.0') # need this to access from the outside world!
