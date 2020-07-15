import pandas as pd
import re
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import json


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


def main():

    table_name = 'idol_datalake_ytchannel'
    dataset_name = bqtable_cfg[table_name]['dataset_name']

    rows = []

    page_num = 18

    for page_idx in tqdm(range(1, page_num, 1)):
        target_url = f'https://ytranking.net/tag/4?p={page_idx}'
        r = requests.get(target_url)
        soup = BeautifulSoup(r.text, 'lxml')
        ul_li = soup.find_all('ul', class_='channel-list')

        x = []
        for ul in ul_li:
            for ps in ul.find_all('p', class_='more'):
                for p in ps:
                    x.append(p)

        for p in x:
            target_channel = p.get("href")
            target_channnel_url = f'https://ytranking.net{target_channel}'
            r = requests.get(target_channnel_url)
            channel_soup = BeautifulSoup(r.text, 'lxml')
            channel_link = channel_soup.find_all('a', href=re.compile('https://www\.youtube\.com'))[-1]
            channel_id = channel_link.get('href').split('/')[-1]

            channel_title = channel_link.text.replace('exit_to_app', '')

            tags_li = channel_soup.find('section', class_='tag')
            tags = [i.text for i in tags_li.find_all('li')]
            tags_str = ','.join(tags)

            if '音楽' in tags and '女性' in tags and '芸能人' in tags:
                rows.append([channel_title, channel_id, tags_str])

    df = pd.DataFrame(rows, columns=['name', 'channel_id', 'tags'])

    # add df to bq
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    table_ref = client.dataset(dataset_name).table(table_name)
    load_job = client.load_table_from_dataframe(
        df, table_ref, job_config=job_config
    )
    load_job.result()
    return


if __name__ == "__main__":
    main()
