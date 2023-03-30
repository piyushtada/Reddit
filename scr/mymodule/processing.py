import json
import os
import shutil
import time

def insert_json_data(file_name, conn):
    # Open the file and load the JSON data
    with open(file_name, 'r') as f:
        data = json.load(f)

    # Create a list of tuples containing the data to be inserted
    data_list = []
    for date, comments in data['comments'].items():
        for comment in comments:
            data_list.append((comment['date'], comment['clean_text'], comment['link_id'], comment['parent_id'], comment['id'], comment['author'], comment['created_utc'], comment['num_comments'], comment['over_18'], comment['is_self'], comment['score'], comment['selftext'], comment['stickied'], comment['subreddit'], comment['subreddit_id'], comment['title']))
    
    # insert post data
    post_list = []
    for date, posts in data['posts'].items():
        for post in posts:
            post_list.append((post['date'], post['clean_text'], post['id'], post['author'], post['created_utc'], post['num_comments'], post['over_18'], post['is_self'], post['score'], post['stickied'], post['subreddit'], post['subreddit_id']))
    # Open a cursor and execute the insert query
    cur = conn.cursor()
    insert_query = "INSERT INTO l0_comments (comment_date, clean_text, link_id, parent_id, comment_id, author, created_utc, num_comments, over_18, is_self, score, selftext, stickied, subreddit, subreddit_id, title) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cur.executemany(insert_query, data_list)

    insert_post_query = "INSERT INTO L0_posts (post_date, clean_text, post_id, author, created_utc, num_comments, over_18, is_self, score, stickied, subreddit, subreddit_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cur.executemany(insert_post_query, post_list)
    # Commit the changes and close the connection
    conn.commit()
    cur.close()
    



def move_file_to_output_folder(file_path, output_folder):
    timestamp = int(time.time())
    file_name = os.path.basename(file_path)
    output_path = os.path.join(output_folder, f"{timestamp}_{file_name}")
    os.makedirs(output_folder, exist_ok=True)
    shutil.move(file_path, output_path)

def process_files_in_folder(input_folder, output_folder, conn):
    os.makedirs(output_folder, exist_ok=True)
    for file_name in os.listdir(input_folder):
        if file_name.endswith(".json"):
            print(f"Processing {file_name}")
            input_file_path = os.path.join(input_folder, file_name)
            insert_json_data(input_file_path, conn)
            move_file_to_output_folder(input_file_path, output_folder)
    conn.close()

