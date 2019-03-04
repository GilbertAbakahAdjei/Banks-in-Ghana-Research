import tweepy
from tweepy import OAuthHandler
import os
import datetime
import numpy as np
import pandas as pd
import pymongo
from pymongo import MongoClient

def authentication():
    try:
        consumer_key = os.environ['CONSUMER_KEY']
        consumer_secret = os.environ['CS_KEY']
        access_token = os.environ['ACCESS_KEY']
        access_secret = os.environ['AS_KEY']     
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_secret)     
        api = tweepy.API(auth)
    except KeyError:
        print("Enter valid twitter app credentials")
    return api

def user_data(username):
    api = authentication()
    data = api.get_user(username)
    created_at = data.created_at
    followers = data.followers_count
    num_tweets = data.statuses_count
    likes = data.favourites_count
    following = data.friends_count
    todays_date = datetime.datetime.now()
    age = (todays_date.year - data.created_at.year)*12 + todays_date.month - data.created_at.month    
    return created_at, followers, num_tweets, likes, following, age

def tweet_properties(username):
    api = authentication()
    start_date = datetime.datetime(2019,2,1,0,0,0)
    end_date = datetime.datetime(2019,3,1,0,0,0)
    count = 0
    tweets_url, media_url = [], []
    nums_likes, num_retweets = [], []
    #Iterate the entire tweets made by user and retrieving items that fall within specified time window
    for tweet in tweepy.Cursor(api.user_timeline, id = username).items():
        if (tweet.created_at > start_date) and (tweet.created_at < end_date):
            tweets_url.append("https://twitter.com/" + username + "/status/" + str(tweet.id))        
            num_retweets.append(tweet.retweet_count)
            nums_likes.append(tweet.favorite_count)
        
            if(tweet.entities.get('media')) != None:
                media_url.append(tweet.entities.get('media')[0].get('media_url_https'))
            else:
                media_url.append(np.nan)       
            count +=1
    #Uncomment next line to check the total number of tweets between the two dates
    #print("There are {} tweets between the dates {} and {}".format(count,start_date, end_date))
    return tweets_url, media_url, nums_likes, num_retweets, count

#Preparing retrieved data to be inserted into database
def prep_to_db(username):
    tweets_url, media_url, nums_likes, num_retweets, _ = tweet_properties(username)
    data = []
    for i in range(len(tweets_url)):
        data.append({"tweet_urls":tweets_url[i], "nums_likes":nums_likes[i], "num_retweets":num_retweets[i],
                            "media_url":media_url[i]})
    return data

#Creating a Mongo Client
client = MongoClient()
#Creating SFL database
SFL_db = client.SFL
#Creating Calbank collection
adb_ghana_collection = SFL_db.adb_ghana_collection
#Inserting prepared data into collection
results = adb_ghana_collection.insert_many(data)

def export_csv(username):
    tweets_url, media_url, nums_likes, num_retweets, count = tweet_properties(username)
    created_at, followers, num_tweets, likes, following, age = user_data(username)
    df = pd.DataFrame({"tweets_url":tweets_url, "media_url":media_url, "tweet_likes":nums_likes, "num_retweets":num_retweets,
                      "tweet_count":count, "account_created_at":created_at, "account_total_tweets":num_tweets,
                      "account_followers":followers, "account_total_likes":likes, "following":following, "Months old":age})
    return df.to_csv(username + '.csv')    