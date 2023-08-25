from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup as bs
import pymongo
from google.cloud import storage
import os
from io import BytesIO
import json
from bson import json_util
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 23),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'cgv_movie_scraper2',
    default_args=default_args,
    description='Scrape CGV movie data2',
    schedule_interval='0 9 * * *',  # 9:00 AM daily
)

def scrape_cgv_movies():
    url = 'http://www.cgv.co.kr/movies/pre-movies.aspx'
    r = requests.get(url)
    CGv = bs(r.text)
    cgv = CGv.select_one('div.sect-movie-chart')

    total = []
    linklist = []
    for i in cgv.select('ol li'):
        linklist.append(i.select('a')[0]['href'])

    for link in linklist:
        cgvDic = {}
        new_url = 'http://www.cgv.co.kr' + link
        r = requests.get(new_url)
        info = bs(r.text)

        # 제목
        cgvDic['title_kr'] = info.select_one('div.box-contents strong').text
        # 개봉일자
        cgvDic['released_At'] = info.select('div.spec dd')[5].text
        # 포스터이미지
        img = info.select_one('div.box-image img')['src']
        cgvDic['poster_img_url'] = img

        # OTT 정보
        cgvDic['OTT'] = 'CGV'
        # 감독정보
        if info.select('div.spec dl dt')[0].text[0] == '감':
            g = []
            for i in info.select_one('div.spec dd').text.split(','):
                if i in '\n':
                    i = i.replace('\n', '')
                if i in '\r':
                    i = i.replace('\r', '')
                if i in '\xa0':
                    i = i.replace('\xa0', '')
                g.append(i.strip())
            cgvDic['direc'] = g

        # 관람등급
        view_grade_info = info.select('div.spec dl dd.on')[1].text
        view_grade = view_grade_info.split(',')[0].strip()
        if str(view_grade) == '전체관람가':
            cgvDic['rating'] = 'ALL'
        elif str(view_grade) == '12세이상관람가':
            cgvDic['rating'] = 'OVER12'
        elif str(view_grade) == '15세이상관람가':
            cgvDic['rating'] = 'OVER15'
        elif str(view_grade) == '18세이상관람가':
            cgvDic['rating'] = 'OVER18'
        else:
            cgvDic['rating'] = 'UNKNOWN'

        # 배우정보
        if info.select('div.spec dl dt')[1].text.split(' ')[1] == '배우':
            g = []
            for i in info.select('div.spec dd.on a'):
                g.append(i.text)
            cgvDic['actor'] = g

        # 장르
        if info.select('div.spec dt')[2].text[0] == '장':
            g = []
            for i in info.select('div.spec dt')[2].text.replace('\xa0', '').split(':')[1:]: # \xa0 특수공백 제거
                g.append(i)
            cgvDic['genres'] = g
        cgvDic['synopsis'] = info.select_one('div.col-detail div.sect-story-movie').text.replace('\n', ' ').replace('\r', ' ')

        cgvDic['info'] = info.select('div.spec dl dd.on')[1].text

        # 베우정보
        if info.select('div.spec dl dt')[1].text.split(' ')[1]:
            g = []
            for i in info.select('div.spec dd.on a'):
                g.append(i.text)
            cgvDic['actor'] = g

        for a in info.select('div.box-contents em'):
            if a.text[0] == 'D':
                cgvDic['Dday'] = a.text
        #개봉예정작
        cgvDic['comming'] = 'TRUE'
        total.append(cgvDic)

    return total

# MongoDB에 저장하는 함수
def mongodb(**kwargs):
    ti = kwargs['ti']
    total = ti.xcom_pull(task_ids='scrape_cgv_movies')
    client = pymongo.MongoClient("mongodb://계정:비밀번호@IP:포트번호")
    db = client["db_name"]
    collection_name = "collection_name"
    if collection_name not in db.list_collection_names():
        db.create_collection(collection_name)
    collection = db[collection_name]

    new_movies = []

    for movie in total:
        existing_movie = collection.find_one({'title_kr': movie['title_kr'], 'OTT': movie['OTT']})
        if existing_movie:
            continue
        movie.pop('href', None)
        collection.insert_one(movie)
        new_movies.append(movie)

    new_movies_json_str = json.dumps(new_movies, default=json_util.default)
    
    ti.xcom_push(key='new_movies_json_str', value=new_movies_json_str)


def upload_image_url_to_gcs(bucket_name, poster_img_url, file_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    response = requests.get(poster_img_url)
    
    with BytesIO(response.content) as f:
        blob.upload_from_file(f, content_type='image/jpeg')
        
def gcp_upload(**kwargs):
    ti = kwargs['ti']
    new_movies_json_str = ti.xcom_pull(key='new_movies_json_str', task_ids='store_to_mongodb')
    logging.info("XCom에서 가져온 JSON 문자열: %s", new_movies_json_str)
    new_movies = json.loads(new_movies_json_str, object_hook=json_util.object_hook)
    client = pymongo.MongoClient("mongodb://계정:비밀번호@IP:포트번호")
    db = client["db_name"]
    collection_name = db["collection_name"]
    
    for movie in new_movies:
        movie=collection_name.find_one({'title_kr': movie['title_kr']})
        movie_id = str(movie['_id'])
        poster_img_url = movie['poster_img_url']
        file_name = f'{movie_id}.jpg'
        bucket_name = 'moochu2'

        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/opt/airflow/my_keyfile.json"
        upload_image_url_to_gcs(bucket_name, poster_img_url, file_name)

scrape_cgv_movies_task = PythonOperator(
    task_id='scrape_cgv_movies',
    python_callable=scrape_cgv_movies,
    provide_context=True,  # total의 변수를 xcom에 저장하여 task instance 사용할수 있게 해준다 
    dag=dag,
)

# MongoDB에 저장하는 PythonOperator
store_to_mongodb_task = PythonOperator(
    task_id='store_to_mongodb',
    python_callable=mongodb,
    provide_context=True,  # total 변수를 XCom으로부터 전달받을 수 있게 해준다
    dag=dag,
)
gcp_upload = PythonOperator(
    task_id = 'gcp_upload',
    python_callable=gcp_upload,
    provide_context=True,
    dag=dag
)

scrape_cgv_movies_task >> store_to_mongodb_task >> gcp_upload

if __name__ == "__main__":
    dag.cli()