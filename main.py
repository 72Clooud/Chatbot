import sqlite3
import json
from datetime import datetime

timeframe = '2015-01'
sql_transaction = []

connection = sqlite3.connect(f'{timeframe}.db')
cursor = connection.cursor()

def create_table():
    cursor.execute("""CREATE TABLE IF NOT EXISTS parent_reply(parent_id TEXT PRIMARY KEY,
                   comment_id TEXT UNIQUE, parent TEXT, comment TEXT, subreddit TEXT,
                   unix INT, score INT)""")
    
def format_data(data):
    data = data.replace("\n", " newlinechar ").replace("\r", " newlinechar ").replace('"', "'")
    return data    

def find_existing_score(pid):
    try:
        sql = f'SELECT score FROM parent_reply WHERE parent_id = "{pid}" LIMIT 1'
        cursor.execute(sql)
        results = cursor.fetchone()
        if results != None:
            return results[0]
        else: return False
    except Exception:
        return False
    
def acceptable(data):
    if len(data.split(" ")) > 50 or len(data) < 1:
        return False
    elif len(data.split(" ")) > 1000:
        return False
    elif data == '[deleted]' or data == '[removed]':
        return False
    else:
        return True
      
            
def find_parent(pid):
    try:
        sql = f'SELECT comment FROM parent_reply WHERE comment_id = "{pid}" LIMIT 1'
        cursor.execute(sql)
        results = cursor.fetchone()
        if results != None:
            return results[0]
        else: return False
    except Exception:
        return False
 
def transaction_builder(sql):
    global sql_transaction
    sql_transaction.append(sql)
    if len(sql_transaction) > 1000:
        cursor.execute('BEGIN TRANSACTION')
        for i in sql_transaction:
            try:
                cursor.execute(i)
            except Exception as e: 
                print(f'SQL transaction error: {str(e)}')
        connection.commit()
        sql_transaction = []
        
        
def sql_insert_replace_comment(comment_id, parent_id, parent, comment, subreddit, time, score, ): 
    try:
        sql = f"""UPDATE parent_reply SET parent_id = "{parent_id}", comment_id = "{comment_id}", parent = "{parent}", comment = "{comment}", subreddit = "{subreddit}", unix = {int(time)}, score = {score} WHERE parent_id = "{parent_id}";"""
        transaction_builder(sql)
    except Exception as e:
        print(f'S-UPDATE insertion {str(e)}')
    
def sql_insert_has_parent(comment_id, parent_id, parent, comment, subreddit, time, score): 
    try:
        sql = f"""INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score) VALUES ("{parent_id}", "{comment_id}", "{parent}", "{comment}", "{subreddit}", {int(time)}, {score})"""
        transaction_builder(sql)
    except Exception as e:
        print(f's-PARENT inseertion {str(e)}')
        
def sql_insert_has_no_parent(comment_id, parent_id, comment, subreddit, time, score): 
    try:
        sql = f"""INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score) VALUES ("{parent_id}", "{comment_id}", "{comment}", "{subreddit}", {int(time)}, {score})"""
        transaction_builder(sql)
    except Exception as e:
        print(f's-NO_PARENT insertion {str(e)}')

if __name__ == "__main__":
    create_table()
    row_counter =  0
    paired_rows = 0
    
    with open(f"D:/NIGA/reddit_data/{timeframe.split('-')[0]}/RC_{timeframe}", buffering=1000) as f:
        for row in f:
            row_counter += 1
            row = json.loads(row)
            parent_id = row['parent_id']
            body = format_data(row['body'])
            created_utc = row['created_utc']
            score = row['score']
            comment_id = row['name']
            subreddit = row['subreddit']
            parent_data = find_parent(parent_id)
            
            if score >= 2:
                if acceptable(body):
                    existing_comment_score = find_existing_score(parent_id)
                    if existing_comment_score:
                        if score > existing_comment_score:
                            sql_insert_replace_comment(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                    else:
                        if parent_data:
                            sql_insert_has_parent(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                            paired_rows += 1
                        else:
                            sql_insert_has_no_parent(comment_id, parent_id, body, subreddit, created_utc, score)
            if row_counter % 100000 == 0:
                print(f"Total rows read: {row_counter}, Paired rows: {paired_rows}, Time: {str(datetime.now())}")