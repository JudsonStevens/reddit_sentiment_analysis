from bs4 import BeautifulSoup
import psycopg2
import requests
import praw
from configparser import ConfigParser
import json
from datetime import datetime

def config(filename='database.ini', section='postgresql'):
  parser = ConfigParser()
  parser.read(filename)

  db = {}
  if parser.has_section(section):
    params = parser.items(section)
    for param in params:
      db[param[0]] = param[1]
  else:
    raise Exception(f'Section {section} not found in the {filename} file')
  return db

def insert(data):
  conn = None
  try:
    params = config()
    print('Connecting to the PostgreSQL database...')
    conn = psycopg2.connect(**params)

    cur = conn.cursor()
    for entry in data:
      comb_gold_count = entry['gildings']['gid_1'] + entry['gildings']['gid_2'] + entry['gildings']['gid_3']
      converted_date = datetime.utcfromtimestamp(entry['created_utc'])
      print('Inserting data...')
      cur.execute("""
                    INSERT INTO submissions_info 
                    (submission_text, submission_id, gold_count, created_at, title_text, number_of_comments, number_of_crossposts, submission_score)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                  """, 
                  (entry['selftext'], entry['id'], comb_gold_count, converted_date, entry['title'], entry['num_comments'], entry['num_crossposts'], entry['score'])
                  )
      conn.commit()

    cur.close()
  except (Exception, psycopg2.DatabaseErrror) as error:
    print(error)
  finally:
    if conn is not None:
      conn.close()
      print('Database connection closed.')

def get_submission_ids():
  conn = None
  reddit = praw.Reddit(client_id='fAMWjQT_fhFInA',
                      client_secret='lAPXKuOZhOIsYGkDA31OcTaoFK8',
                      user_agent= 'SentimentBot-AI',
                      user_name= 'SentimentBot-AI',
                      password= '1qaz2wsx!QAZ@WSX')
  try:
    params = config()
    print('Connecting to the PostgreSQL database...')
    conn = psycopg2.connect(**params)

    cur = conn.cursor()
    cur.execute("""
                  SELECT submission_id
                  FROM submissions_info;
                """)
    ids = cur.fetchall()

    for element in ids[7089:9000]:
      try:
        submission = reddit.submission(f"{element[0]}")
        submission.comments.replace_more(limit=None)
        for comment in submission.comments.list():
          banned = False if comment.banned_at_utc is None else True
          date_banned = None if comment.banned_at_utc is None else datetime.utcfromtimestamp(comment.banned_at_utc)
          converted_date = datetime.utcfromtimestamp(comment.created_utc)
          comb_gildings = comment.gildings['gid_1'] + comment.gildings['gid_2'] + comment.gildings['gid_3']
          flair_type = '' if comment.author_flair_type is None else comment.author_flair_type
          print('Inserting data...')
          cur.execute("""
                        INSERT INTO comment_info
                        (author_banned, date_banned, flair_text, flair_type, author_name, comment_body, controversiality, created_at, gilded, gildings, comment_id, comment_score, stickied, number_of_downvotes, number_of_upvotes)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                      """,
                      (banned, date_banned, comment.author_flair_text, flair_type, comment.author_fullname, comment.body, comment.controversiality, converted_date, comment.gilded, comb_gildings, comment.id, comment.score, comment.stickied, comment.downs, comment.ups)
                      )
          conn.commit()
      except Exception as error:
        print(error)
        cur.execute("""
              INSERT INTO comment_info
              (author_banned, date_banned, flair_text, flair_type, author_name, comment_body, controversiality, created_at, gilded, gildings, comment_id, comment_score, stickied, number_of_downvotes, number_of_upvotes)
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (banned, date_banned, comment.author_flair_text, '', '', comment.body, comment.controversiality, converted_date, comment.gilded, comb_gildings, comment.id, comment.score, comment.stickied, comment.downs, comment.ups)
            )
        conn.commit()
        print('Flair type modified record saved')
      continue
    cur.close()    
  except Exception as error:
    print(error)
  finally:
    if conn is not None:
      conn.close()
      print('Database connection closed.')

def main():
  ### Pulled submissions ranging from 15 days ago to 60 so far. That's 2018-10-01 to 2018-11-21
  base_url = 'https://api.pushshift.io/reddit/search/submission/?'
  bitcoin_subreddit = 'subreddit=bitcoin'
  litecoin_subreddit = 'subreddit=litecoin'
  btc_subreddit = 'subreddit=btc'
  ethereum_subreddit = 'subreddit=ethereum'
  after_days = '16'
  before_days = '15'
  number_of_submissions = 'size=500'
  fields = 'fields=selftext,title,gildings,id,created_utc,num_comments,num_crossposts,score'


  while after_days < '34':

    try:
      print(f"This is after {after_days} and before {before_days} days ago")

      url = base_url + bitcoin_subreddit + '&' + 'after=' + after_days + 'd&' + 'before=' + before_days + 'd&' + fields + '&' + number_of_submissions

      response = requests.get(url)

      soup = BeautifulSoup(response.content, features="html.parser")

      results_dict = json.loads(str(soup))

      insert(results_dict['data'])

      after_days_num = int(after_days)
      after_days_num += 1
      after_days = str(after_days_num)

      before_days_num = int(before_days)
      before_days_num += 1
      before_days = str(before_days_num)
    except KeyError as error:
      print(error)
      continue

if __name__ == '__main__':
  # main()
  get_submission_ids()