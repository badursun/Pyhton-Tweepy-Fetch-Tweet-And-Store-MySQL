from __future__ import absolute_import, print_function
from tweepy import OAuthHandler, Stream, StreamListener

import simplejson as json
import mysql.connector
import unicodedata
from unidecode import unidecode

#---------------------------------
# Go to http://apps.twitter.com and create an app.
# The consumer key and secret will be generated for you after
#---------------------------------
consumer_key        ="CONSUMER_KEY"
consumer_secret     ="CONSUMER_SECRET"

#---------------------------------
# After the step above, you will be redirected to your app's page.
# Create an access token under the the "Your access token" section
#---------------------------------
access_token        ="ACCESS_TOKEN"
access_token_secret ="ACCESS_TOKEN_SECRET"

#---------------------------------
# HOOPII APP ACCOUNT ID // Only For me =)
#---------------------------------
ACCOUNT_KEY_ID      = 1

#---------------------------------

# MYSQL
#---------------------------------
HOST                = "MYSQL_SERVER_IP"
USER                = "MYSQL_USER"
PASSWD              = "MYSQL_PASSWORD"
DATABASE            = "MYSQL_DATABASE_NAME"

import tweepy

#---------------------------------
# FETCH COUNT
#---------------------------------
MAX_TWEETS = 4000

auth 	= tweepy.OAuthHandler(consumer_key, consumer_secret)
api 	= tweepy.API(auth, wait_on_rate_limit=True)

#---------------------------------
# MySQL Connection // utf8mb4 For Emoji Ascii
#---------------------------------
db = mysql.connector.connect(host=HOST, user=USER, passwd=PASSWD, db=DATABASE, charset='utf8', use_pure=True )
cursor = db.cursor()
cursor.execute('SET NAMES utf8mb4')
cursor.execute("SET CHARACTER SET utf8mb4")
db.commit()
cursor.close()

#---------------------------------
# MySQL Insert Definiation
#---------------------------------
def store_data(created_at, text, screen_name, tweet_id):
    cursor = db.cursor()
    insert_query = "INSERT INTO tbl_tweets(TWEET_ID, SCREEN_NAME, CREATED_DATE, MESSAGE, ACCOUNT_ID) VALUES(%s, %s, %s, %s, %s)"
    cursor.execute(insert_query, (tweet_id, screen_name, created_at, text, ACCOUNT_KEY_ID))
    db.commit()
    cursor.close()
    return

#---------------------------------
# API Loop
#---------------------------------
for tweet in tweepy.Cursor(api.search, q='havaist', count=MAX_TWEETS, tweet_mode='extended').items(MAX_TWEETS):
    try:         
        store_data( tweet.created_at, tweet.full_text.encode('utf-8'), tweet.user.screen_name , tweet.id )
        print("--TWEET INSERT-- ID: ", tweet.id )
        pass
        continue
    except tweepy.TweepError as e:  
            print(e.reason)
            sleep(900)
            continue
    except StopIteration: #stop iteration when last tweet is reached
            break

db.close()
