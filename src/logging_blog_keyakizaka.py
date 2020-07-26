
import pandas as pd
import json
import datetime
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import time
import schedule

from google.cloud import bigquery
from google.oauth2 import service_account

secret_dir = '../secret'

with open(f'../config/bqtable_cfg.json', 'r') as f:
    bqtable_cfg = json.load(f)

# gcp setting
key_path = f'{secret_dir}/tkhr-free-myaun.json'
credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
client = bigquery.Client(
    credentials=credentials,
    project=credentials.project_id,
)


def read_latest_savedblog_ts_from_bq():

    table_name = 'keyakizaka46__datalake__blog'
    project_name = bqtable_cfg[table_name]['project_name']
    dataset_name = bqtable_cfg[table_name]['dataset_name']

    QUERY = (
        f'SELECT page_id, timestamp, title FROM `{project_name}.{dataset_name}.{table_name}` '
        'ORDER BY timestamp DESC LIMIT 1'
    )
    query_job = client.query(QUERY)
    rows = query_job.result()
    df_blogs = pd.DataFrame([[row.page_id, row.timestamp, row.title] for row in rows], columns=['id', 'timestamp', 'title'])
    latest_savedblog_ts = df_blogs['timestamp'].values[0]
    latest_savedblog_ts = pd.to_datetime(latest_savedblog_ts)
    return latest_savedblog_ts


def insert_keyakizaka_blog():

    max_page_number = 455
    latest_date = read_latest_savedblog_ts_from_bq()
    end_flag = False

    all_articles = []
    for page_idx in tqdm(range(max_page_number)):
        target_url = f'http://www.keyakizaka46.com/s/k46o/diary/member/list?ima=0000&page={page_idx}&cd=member'
        r = requests.get(target_url)
        page_soup = BeautifulSoup(r.text, 'lxml')

        for i, j in enumerate(page_soup.select('article')):
            author = j.p.text.replace(' ', '').replace('\n', '')
            datetime_str = j.find('div', class_="box-bottom").li.text.replace(' ', '').replace('\n', '')
            datetime_save = datetime.datetime.strptime(datetime_str, '%Y/%m/%d%H:%M')
            title = j.find('div', class_="box-ttl").a.text
            body = j.find('div', class_="box-article")
            text = body.text
            page_url = 'http://www.keyakizaka46.com' + j.find('li', class_="singlePage").a.get('href')
            page_id = page_url.split('/')[-1].split('?')[0]
            images = ''
            for img in body.find_all('img'):
                images += '%s\t' % img.get('src')

            if datetime_save > latest_date:
                all_articles.append([page_id, author, datetime_save, title, text, images, page_url])
                continue
            else:
                end_flag = True
                break
        if end_flag is True:
            break

    df = pd.DataFrame(all_articles, columns=['page_id', 'author', 'timestamp', 'title', 'text', 'images', 'url'])

    # add df to bq
    table_name = 'keyakizaka46__datalake__blog'
    dataset_name = bqtable_cfg[table_name]['dataset_name']

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    table_ref = client.dataset(dataset_name).table(table_name)
    load_job = client.load_table_from_dataframe(
        df, table_ref, job_config=job_config
    )
    load_job.result()
    print(f'Complete job! {len(df)} blog added')

    return


if __name__ == "__main__":

    # insert_keyakizaka_blog()
    schedule.every(6).hours.do(insert_keyakizaka_blog)
    while True:
        schedule.run_pending()
        time.sleep(10)
