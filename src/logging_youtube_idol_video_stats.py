import sys
import json
import pandas as pd
import time
import datetime
import schedule

# import timeout_decorator
from time import sleep
RETRY = 2

from apiclient.discovery import build

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

sys.path.append(secret_dir)
import devkey
DEVELOPER_KEY = devkey.api2
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

# connection build
YT = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=DEVELOPER_KEY)


def get_channel_vlist_quick(yt, cid, max_num):
    channel_videos_tmp = []
    nextPageToken = ''
    pageNum = 50
    while 1:
        for ridx in range(RETRY):
            try:
                feed = yt.search().list(
                    channelId=cid, maxResults=pageNum,
                    order='date', type='video', part='id', pageToken=nextPageToken
                ).execute()
                break
            except Exception as e:
                print(ridx, e, '...')
                sleep(5)
                continue
        if ridx == RETRY - 1:
            print('retry error ...')
            break
        vids = [i['id']['videoId'] for i in feed.get('items')]
        channel_videos_tmp.extend(vids)
        if 'nextPageToken' in feed:
            nextPageToken = feed['nextPageToken']
        else:
            break
        if len(channel_videos_tmp) > (max_num - 60):
            pageNum = (pageNum / 2) + 1
        if len(channel_videos_tmp) > max_num:
            break
    print(f"{cid} ... get {len(channel_videos_tmp)} videos")
    return channel_videos_tmp


def get_video_stats(vid):
    try:
        video_detail = YT.videos().list(part="id,snippet,statistics", id=vid).execute()
        return video_detail.get("items", [])[0]
    except:
        print('skipped')
        return None


def insert_channel_videos_stats():

    channel_ids = [
        'UCUzpZpX2wRYOk3J8QTFGxDg',  # nogizaka
        'UCmr9bYmymcBmQ1p2tLBRvwg',  # keyakizaka
        'UCR0V48DJyWbwEAdxLL5FjxA',  # hinatazaka
    ]

    rows = []
    for channel_id in channel_ids:

        channel_videos = get_channel_vlist_quick(YT, channel_id, 1000)
        now = datetime.datetime.now()

        for vidx, vid in enumerate(channel_videos):
            video_detail = get_video_stats(vid)
            sleep(1)
            if video_detail is None:
                continue
            rows.append([
                now, video_detail['snippet']['channelId'], video_detail['id'], video_detail['snippet']['title'], video_detail['snippet']['publishedAt'],
                video_detail['statistics'].get("viewCount", None), video_detail['statistics'].get("likeCount", None), video_detail['statistics'].get("dislikeCount", None), video_detail['statistics'].get("commentCount", None)

            ])

    col_names = [
        'timestamp', 'channel_id', 'video_id', 'title', 'published_at', 'view_count', 'like_count', 'dislike_count', 'comment_count'
    ]
    df = pd.DataFrame(rows, columns=col_names)

    # add df to bq
    table_name = 'idol_datalake_ytvideo_dailystats'
    dataset_name = bqtable_cfg[table_name]['dataset_name']

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    table_ref = client.dataset(dataset_name).table(table_name)
    load_job = client.load_table_from_dataframe(
        df, table_ref, job_config=job_config
    )
    load_job.result()
    print(f'Complete job!')
    return


if __name__ == "__main__":

    schedule.every(24).hours.do(insert_channel_videos_stats)
    while True:
        schedule.run_pending()
        time.sleep(10)
