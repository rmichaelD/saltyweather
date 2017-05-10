import requests
import requests.auth
from datetime import datetime
import pymysql
import hashlib
import sys
import time
import re

def get_cnx_to_db():
    try:
        cnx = pymysql.connect(user="", password="", 
                                      host="localhost", unix_socket="/var/run/mysqld/mysqld.sock", 
                                      database="salty_weather", charset='utf8')
    except pymysql.Error as err:
        print(err)
    else:
        print("Connection to DB successful")
        return cnx

def print_data(data):
    author = data['author']
    title = data['title']
    domain = data['domain']
    permalink = data['permalink']
    url = data['url']
    created_utc = data['created_utc']
    date = datetime.fromtimestamp(created_utc)
    score = data['score']
    print(author)
    print(title)
    print(domain)
    print(permalink)
    print(url)
    print(date)
    print(score)
    
def is_thumb(db_key):
    cursor = cnx.cursor()
    find_link = "SELECT COUNT(1) FROM overwatch_highlights WHERE id = %s"
    cursor.execute(find_link, db_key)
    return cursor.fetchone()[0]
    
def get_thumbnail(db_key, url):
    if is_thumb(db_key):
        return None
    else:
        img_link = "http://thumbs.gfycat.com/" + url.split('/')[3] + "-thumb360.jpg"
    print(img_link)
    r = requests.get(img_link, stream = True)
    thumb_data = bytearray()
    for chunk in r:
        thumb_data = thumb_data + chunk
    return thumb_data
    
def add_highlight(data):
#    print("--Adding highlight to DB")
    
    author = data['author']
    title = data['title']
    domain = "gfycat.com"#data['domain']
    permalink = data['permalink'].split('?ref')[0]
    url = data['url']
    url = url.split(' ')[0]
    created_utc = data['created_utc']
    date = datetime.fromtimestamp(created_utc)
    score = data['score']
    title_hash = hashlib.sha1(title.encode()).hexdigest()
    db_key = str(date.year)+str('%02d' % date.month)+str('%02d' % date.day)+title_hash[:5]
    thumbnail = get_thumbnail(db_key, url)
    
#    print("---id: " + db_key)
#    print("---date: " + str(date))
#    print("---title: " + title)
    
    cursor = cnx.cursor()
    add_data = """INSERT INTO overwatch_highlights
                (id, author, title, domain, permalink, url, date, score, thumbnail)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE score=VALUES(score)"""
    data = (db_key, author, title, domain, permalink, url, date, score, thumbnail)
    cursor.execute(add_data, data)
    try:
        cnx.commit()
    except:
        print("-Commit has failed")       
    cursor.close()
    
def find_highlights(threads):
    print("-Finding highlights...")
    nb_highlights = 0
    for thread in threads:
        data = thread['data']
        if data['link_flair_css_class'] == 'a' and data['domain'] == 'gfycat.com':
            add_highlight(data)
            nb_highlights += 1
    print("-Number of highlights found: " + str(nb_highlights))

def get_token():
    client_auth = requests.auth.HTTPBasicAuth('OcOSaoNoZh9jHA', '9P7sd4fvtwKcL30lEEDLN-a-coc')
    post_data = {"grant_type": "password", "username": "", "password": ""}
    headers = {"User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:45.0) Gecko/20100101 Firefox/45.0"}
    response = requests.post("https://www.reddit.com/api/v1/access_token", auth=client_auth, data=post_data, headers=headers)
    json_response = response.json()
    token = json_response['token_type'] + ' ' + json_response['access_token']
    return token

def get_subreddit_threads(subreddit_name, sort, limit = 2000):
    if limit == 0:
        limit = sys.maxsize
    print("Getting subreddit threads...")
    token = get_token()
    headers = {"Authorization": token, "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:45.0) Gecko/20100101 Firefox/44.0"} 
    nb_threads = 0
    after = ''
    threads = []
    while nb_threads < limit and after != 'None':
        if limit - nb_threads < 100:
            params_limit = limit-nb_threads
        else:
            params_limit = 100
        params = {"limit":str(params_limit), 'after': after, 't': 'week', 'restrict_sr': 1}
        response = requests.get("https://oauth.reddit.com/r/" + subreddit_name + "/" + sort, headers=headers, params = params)
        json_response = response.json()
        try:
            threads = json_response['data']['children']
            find_highlights(threads)
            nb_threads += params_limit
            after = json_response['data']['after']
            print("---------------------")
            if len(threads) > 0:
                print_data(threads[len(threads)-1]['data'])
        except RuntimeError as e:
            print(e)
        print("---------------------")
        print("Number of threads retrieved: " + str(nb_threads))
        print("For page token: " + str(after))
        print("=====================")
        time.sleep(2)
    
try:
    cnx = get_cnx_to_db()
    
    while True:
        print("=======================================")
        print("Query began at " + str(datetime.now()))
        print("=======================================")
        start = time.time()
        for sort in ['new', 'hot', 'top']:
            get_subreddit_threads('overwatch', sort)
        end = time.time()
        print("=======================================")
        print("Query ended at " + str(datetime.now()) + " and last for " + str(end - start) + " secondes")
        print("=======================================")
        time.sleep(360)
finally:
    cnx.close()
