
import pandas as pd
import json
import datetime
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import time
import schedule
import re
from dateutil.relativedelta import relativedelta

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

table_name = 'nogizaka46__datalake__blog'

def read_latest_savedblog_ts_from_bq():

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


def get_month_list( start, end ):

    start_dt = datetime.datetime.strptime(start, "%Y%m")
    end_dt = datetime.datetime.strptime(end, "%Y%m")

    lst = []
    t = start_dt
    while t <= end_dt:
        lst.append(t)
        t += relativedelta(months=1)

    return [x.strftime("%Y%m") for x in lst]


def insert_nogizaka_blog():

    headers = {'User-Agent': 'Mozilla/5.0'}
    
    # latest_date = read_latest_savedblog_ts_from_bq()

    start = '201111'
    end = '202007'
    month_list = get_month_list(start, end)
    end_flag = False

    all_articles = []
    for ymonth in tqdm(month_list):

        target_url = f'http://blog.nogizaka46.com/?p=0&d={ymonth}'
        r = requests.get(target_url, headers=headers)
        soup = BeautifulSoup(r.text, 'lxml')
        max_page_number = int(soup.find('div', class_='paginate').find_all('a')[-2].text.replace(u"\xa0",u""))  # これでいいのか?
        
        for page_idx in range(1, max_page_number+1, 1):
            
            target_url = f'http://blog.nogizaka46.com/?p={page_idx}&d={ymonth}'
            r = requests.get(target_url, headers=headers)
            page_soup = BeautifulSoup(r.text, 'lxml')

            titles = [i.find('span', class_='entrytitle').text for i in page_soup.find_all('h1', class_='clearfix')]
            authors = [i.text for i in page_soup.find_all('span', class_='author')]
            datetimes = [i.text.split('｜')[0].replace(' ','').replace('\n','') for i in page_soup.find_all('div', class_='entrybottom')]
            page_urls = [i.find('a').get('href') for i in page_soup.find_all('div', class_='entrybottom')]
            page_ids = [i.split('/')[-1].split('.')[0] for i in page_urls]
            texts = []
            images = []
            for i, j in enumerate(page_soup.find_all('div', class_='entrybody')):  # 画像URL抽出 ... アドホックな処理多いので注意
                image_urls = []
                for img in j.find_all('img', src=re.compile("^http(s)?://img.nogizaka46.com/blog/")):
                    if img.get('src')[-4:] == '.gif':
                        continue
                    if not img.get('class') == None:
                        continue
                    image_urls.append(img.get('src'))
                image_str = ''
                for img in image_urls: image_str += '%s\t'% img
                texts.append( j.text )
                images.append( image_str )

            for i in range(len(titles)):
                all_articles.append([page_ids[i], authors[i], datetimes[i], titles[i], texts[i], images[i], page_urls[i]])
            
            # page_articles = get_nogi_articles_from_single_page(soup)
            # all_articles.extend(page_articles)

    df = pd.DataFrame(all_articles, columns=['page_id', 'author', 'timestamp', 'title', 'text', 'images', 'url'])

    # add df to bq
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

    insert_nogizaka_blog()
    # schedule.every(6).hours.do(insert_nogizaka_blog)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(10)
