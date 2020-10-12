import httplib

# SERVER = 'localhost:5000'



def get_counts_per_reviewername():
    out = dict()
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/reviews/reviewername/counts')
    resp = h.getresponse()
    out = resp.read()
    return out

def get_counts_by_reviewername(reviewername):
    out = dict()
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/reviews/reviewername/counts/'+reviewername)
    resp = h.getresponse()
    out = resp.read()
    return out

def get_avg_overall_per_asin():
    out = dict()
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/reviews/overall/avg_overall')
    resp = h.getresponse()
    out = resp.read()
    return out

def get_avg_overall_by_asin(asin):
    out = dict()
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/reviews/overall/avg_overall/'+asin)
    resp = h.getresponse()
    out = resp.read()
    return out

def get_helpful_per_user():
    out = dict()
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/reviews/helpful/fraction')
    resp = h.getresponse()
    out = resp.read()
    return out

def get_avg_price_per_title():
    out = dict()
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/meta/title/avg_price')
    resp = h.getresponse()
    out = resp.read()
    return out

def get_avg_price_by_title(title):
    out = dict()
    h = httplib.HTTPConnection(SERVER)
    title = title.replace(" ","%20")
    h.request('GET', 'http://'+SERVER+'/meta/title/avg_price/'+title)
    resp = h.getresponse()
    out = resp.read()
    return out


if __name__ == '__main__':
    print "************************************************"
    print "my flask app of amazon beauty data sets running at ", SERVER
    print "created by Tracy"
    print "************************************************"
    print " "
    print "******** counts per reviewername **********"
    print get_counts_per_reviewername()
    print " "
    print "******** counts at reviewername liz **********"
    print get_counts_by_reviewername('liz')
    print "******** average overall per asin **********"
    print get_avg_overall_per_asin()
    print " "
    print "******** average overall by 130414089X **********"
    print get_avg_overall_by_asin('130414089X')
    print " "
    print "******** counts of the helpful per user **********"
    print get_helpful_per_user()
    print " "
    print "******** average price per title **********"
    print get_avg_price_per_title()
    print " "
    print "******** average price by Stephanie Johnson Mermaid Round Snap Mirror **********"
    print get_avg_price_by_title('Stephanie Johnson Mermaid Round Snap Mirror')
