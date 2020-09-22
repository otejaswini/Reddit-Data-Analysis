import requests
import json
import time
import csv
from urllib.parse import urlparse
import urllib.request as req
import datetime
import socket

imgDownload = "imageurls.csv"

with open(imgDownload, 'w', encoding='utf8') as f:
    writer = csv.writer(f)
    writer.writerow(['url'])

PUSHSHIFT_REDDIT_URL = "https://api.pushshift.io/reddit"

def fetchObjects(**kwargs):
    params = {
        "sort_type": "created_utc",
        "sort": "asc",
        "size": 1000,
        "subreddit":'suicidewatch',
       # "q":'python'
    }

    for key, value in kwargs.items():
        params[key] = value
    print(params)

    type = "comment"
    if 'type' in kwargs and kwargs['type'].lower() == "submission":
        type = "submission"

    r = requests.get(PUSHSHIFT_REDDIT_URL + "/" + type + "/search/", params=params, timeout=30)

    if r.status_code == 200:
        response = json.loads(r.text)
        data = response['data']
        sorted_data_by_id = sorted(data, key=lambda x: int(x['id'], 36))
        return sorted_data_by_id
#To extract the json data for comments or submissions
def extract_reddit_data(filename, **kwargs):

    max_created_utc = 1262385911
    max_id = 0

    file = open(filename, "a")

    while 1:
        nothing_processed = True
        objects = fetchObjects(**kwargs, after=max_created_utc)
        try:
            for object in objects:
                    id = int(object['id'], 36)
                    if id > max_id:
                        nothing_processed = False
                        created_utc = object['created_utc']
                        max_id = id
                        if created_utc > max_created_utc: max_created_utc = created_utc
                        print(json.dumps(object, sort_keys=True, ensure_ascii=True), file=file)
            if nothing_processed: return
        except TypeError:
            pass
        max_created_utc -= 1

        time.sleep(.5)
#To extract specific columns from submissions json
def extract_submissions(filename):
    submissionsList = []
    print("Started Reading JSON file which contains multiple JSON document")
    with open(filename) as f:
        for jsonObj in f:
            submissionDICT = json.loads(jsonObj)
            submissionsList.append(submissionDICT)

    for submission in submissionsList:
        with open("submission_CSV.csv", 'a', newline='', encoding='utf-8') as file:
            a = csv.writer(file, delimiter=',')
            headers = [submission["author"]]
            a.writerow(headers)
        print(submission["author"])
#To extract imageurls from submissions json
def extract_imageurls(filename):
    submissionsList = []
    print("Started Reading JSON file which contains multiple JSON document")
    with open(filename) as f:
        for jsonObj in f:
            submissionDICT = json.loads(jsonObj)
            submissionsList.append(submissionDICT)

    for submission in submissionsList:
        headers = [submission["url"]]
        with open(imgDownload, 'a', newline='', encoding='utf-8') as file:
            a = csv.writer(file, delimiter=',')
            a.writerow(headers)
        print(submission["url"])


#To extract specific columns from comments json
def extract_comments(filename):
    commentsList = []
    print("Started Reading JSON file which contains multiple JSON document")
    with open(filename) as f:
        for jsonObj in f:
            commentsDICT = json.loads(jsonObj)
            commentsList.append(commentsDICT)

    print("Printing each JSON Decoded Object")
    for comments in commentsList:
        with open("comment_CSV.csv", 'a', newline='', encoding='utf-8') as file:
            a = csv.writer(file, delimiter=',')
            headers = [comments["author"]]
            a.writerow(headers)
        print(comments["author"])

#To download the images from the extracted image urls
def imageDownload():
    count = 0
    with open(imgDownload, "r") as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=",")
        for lines in csv_reader:
            if 'imgur' or '.jpg' or '.png' in lines['url']:
                o = urlparse(lines['url'].strip())
                print(o.geturl())
                imgurl = o.geturl()
                with open("imageSave.csv", 'a', newline='', encoding='utf-8') as file:
                    a = csv.writer(file, delimiter=',')
                    try:
                        req.urlretrieve(imgurl, "Current Directory/image_name" + str(count) + '.jpg')
                        headers = [imgurl, "Saved"]
                        a.writerow(headers)
                    except:
                        headers = [imgurl, "Not Saved"]
                        a.writerow(headers)
                        count = count + 1
#To extract basic statistics from submissions/comments json
def extract_stats(filename):
    List = []
    dates = []
    authors = []
    subRedditID = []
    print("Started Reading JSON file which contains multiple JSON document")
    with open(filename) as f:
        for jsonObj in f:
            DICT = json.loads(jsonObj)
            List.append(DICT)
    for jsonObject in List:
        authors.append(jsonObject["author"])
        subRedditID.append(jsonObject["subreddit_id"])
        timestamp = datetime.datetime.fromtimestamp(jsonObject["created_utc"])
        dates.append(timestamp.strftime('%Y-%m-%d'))

    y = len(dates) - 1
    print(min(dates), max(dates))
    print("Number of json objects: "+ str(len(List)))
    print("Number of users: "+str(len(set(authors))))
    print("Number of subreddits: "+str(len(set(subRedditID))))
extract_reddit_data("SW_Comments.json",type="comment")
#extract_submissions("***submission csv***")
#extract_comments("***comment csv***")
#extract_imageurls("Submissions.json")
# extract_stats("SW_Submissions.json")
#extract_stats("comments.json")
#imageDownload()
