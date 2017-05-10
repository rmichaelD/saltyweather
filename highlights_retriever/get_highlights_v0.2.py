# From v0.1.1, execution time went from 250 sec to 10 sec (Thanks PRAW)
#              number of line went from 157 to 137 with comments

import praw
import requests
from datetime import datetime
import pymysql
import hashlib
import time
import sys

bot_username = ''
bot_password = ''

#   Establish a connection to the mysql database
#   Return a pymysql connector to the database
def get_cnx_to_db():
    user = ""
    password = ""
    host = "localhost"
    unix_socket = "/var/run/mysqld/mysqld.sock"
    database = "salty_weather"
    charset = "utf8"
    try:
        cnx = pymysql.connect(user=user, password=password, host=host, 
                              unix_socket=unix_socket, database=database, charset=charset)      # Get the pymysql connector
    except pymysql.Error as err:
        print(err)
        sys.exit(1)
    else:
        print("Connection to DB successful")
        return cnx
    
#   Check if submission already exist
#   Take db_key (submission's database key)
#   Return 1 if submission exist
def is_duplicate(db_key):
    cursor = cnx.cursor()       # Cursor to navigate database
    find_link = "SELECT COUNT(1) FROM overwatch_highlights WHERE id = %s"       # Query to find submission
    cursor.execute(find_link, db_key)       # Execute query
    return cursor.fetchone()[0]         # Retrieve database result
    
    
#   Retrieve thumbnail from Gfycat (Check API at https://gfycat.com/api)
#   Take submission db_key and url
#   Return bytearray of a thumbnail or None if submission already exist
def get_thumbnail(db_key, url):
    if is_duplicate(db_key):        # Check if submission already exsit
        return None
    else:
        img_link = "http://thumbs.gfycat.com/" + url.split('/')[3] + "-thumb360.jpg"        # Format link to retrieve thumbnail
        r = requests.get(img_link, stream = True)       # Retrieve link with requests
        thumb_data = bytearray()        # Initialize array to hold thumbnail
        for chunk in r:
            thumb_data = thumb_data + chunk         # Add thumbnail data into bytearray
        return thumb_data

#   Add the submission to the overwatch_highlights database
#   Take a praw.objects.Submission
def add_highlight(submission):
    author = submission.author.name         # Author's name
    title = submission.title        # Title
    domain = submission.domain      # Domain
    permalink = submission.permalink.split('?ref')[0]       # Permalink with "?ref=search" stripped
    url = submission.url        # Raw link
    url = url.split(' ')[0]         # Link's URL formatted to ignore occasional space in link
    created_utc = submission.created_utc        # Submission time in Unix timestamp
    date = datetime.fromtimestamp(created_utc)      # Convert Unix timestamp to human form
    score = submission.score        # Reddit's score
    title_hash = hashlib.sha1(title.encode()).hexdigest()       # First part of the id is hash of the title
    db_key = str(date.year)+str('%02d' % date.month)+str('%02d' % date.day)+title_hash[:5]      # Seconde part of the id is date
    thumbnail = get_thumbnail(db_key, url)      # Get thumbnail
    
    if thumbnail:       # If thumb not None then submission is not a duplicate
        print("Adding submission :-> "+str(submission))        # Print status update
    
    cursor = cnx.cursor()       # Cursor to navigate database
    
    add_data = """INSERT INTO overwatch_highlights
                (id, author, title, domain, permalink, url, date, score, thumbnail)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE score=VALUES(score)"""          # Query to execute
    data = (db_key, author, title, domain, permalink, url, date, score, thumbnail)      # Data used in query
    cursor.execute(add_data, data)          # Execute query
    try:
        cnx.commit()        # Commit to database changes
    except:
        print("-Commit has failed")       
    cursor.close()
    
#   Retrieve subreddit submission according to search params   
#   Take an praw object
#   Return generator object holding subreddit's submissions
def get_submissions(r):
    search_parms = 'flair:"highlight" site:"gfycat.com"'   # List of search parameter, see https://www.reddit.com/wiki/search
    subreddit='overwatch'   # The subreddit to search
    sort='new'  # Sort the result by {hot, new, top, comments}
    period = 'day'  # Period to search {hour, day, week, month, year, all}
    limit = None    # The highest number of result to be retreive (max 1000)
    return r.search(search_parms, subreddit=subreddit, sort=sort, period=period, limit=limit)    # Return submissions according to params

#   Get a connection to reddit 
#   Return a praw.Reddit object
def get_praw(bot_username, bot_password):
    UA = 'Overwatch highlights retreiver from http://www.saltyweather.com. Contact me at /u/Retroshaft or rmichael.deschenes@email.com'
    r = praw.Reddit(UA)
    r.login(bot_username, bot_password)
    return r
 
#   Print a message indicating the time at which the query began
#   Return a Unix timestamp of the current time
def start_timer():
    print("=======================================")
    print("Query began at " + str(datetime.now()))
    print("=======================================")
    return time.time()
    
#   Print a message indicating the time at which the query end and how long it last
#   Take a Unix timestamp of the starting time
def end_timer(start):
    end = time.time()
    print("=======================================")
    print("Query ended at " + str(datetime.now()) + " and last for " + str(end - start) + " secondes")
    print("=======================================")
    
try:
    cnx = get_cnx_to_db()   #Get connection to the mysql database
    
    while True:
        start = start_timer() # Print and get starting time
        r = get_praw(bot_username, bot_password)    # Get the praw object with user {username, password}
        submissions = get_submissions(r)     # Get subrredit submissions
    
        for submission in submissions:      # Iterate over the subreddit's submissions
            add_highlight(submission)       # Add submission to highlight database
        end_timer(start)
        time.sleep(600)
finally:
    cnx.close()
