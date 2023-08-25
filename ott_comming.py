from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import json
from bson import json_util
import logging
import requests
from bs4 import BeautifulSoup
import pymongo
from google.cloud import storage
import os
from io import BytesIO


default_args = {
    'owner': 'airflow',                  # DAG의 소유자
    'depends_on_past': False,            # 이전 실행이 성공하든 실패하든 실행(이전 실행에 대한 의존여부 지정)
    'start_date': datetime(2023, 8, 1),  # 시작날짜
    'retries': 1,                        # 실패시 재시도 횟수
    'retry_delay': timedelta(minutes=5), # 실패시 다음시도까지 대기 시간
}

dag = DAG(
    'ott_comming_update',                # DAG이름
    default_args=default_args,
    description='Scrape OTT movie data', # DAG설명
    schedule_interval='30 1 * * *',      # 시작시간
)


def fetch_movies_data(url):
    base_url = "https://movie.daum.net"

    response = requests.get(url)
    html_content = response.content
    soup = BeautifulSoup(html_content)
    movie_list=soup.find('ul', class_='list_movieranking aniposter_ott')

    movies = []
    if movie_list is not None:
        for movie in movie_list.find_all('li'):
            movie_dic={}
            href = movie.find('a', class_='thumb_item')['href']
            full_href = base_url + href
            movieId = href.split('=')[-1]
            movie_dic['href'] = f"{base_url}/api/movie/{movieId}/main"
            movie_dic['released_At']= movie.find('span', class_='txt_num').text
            movie_dic['title_kr'] = movie.find('a', class_='link_txt').text
            movie_dic['poster_img_url'] = movie.find('img', class_='img_thumb')['src']
            
            # Determine OTT based on the URL
            ott = 'Unknown'
            if 'watcha' in url:
                movie_dic['OTT'] = 'Watcha'
            elif 'netflix' in url:
                movie_dic['OTT'] = 'Netflix'
            
            movies.append(movie_dic)
            
    return movies

collection_names = [
    'daumwatcha',
    'daumnetflix',
]

def update_movies_data(movies):
    total=[]
    for movie in movies:
        response = requests.get(movie['href'])
        movie_info = response.json()['movieCommon']
        
        title_english = movie_info['titleEnglish']
        plot = movie_info['plot']
        genres = movie_info['genres']
        
        admission_code = None
        for info in movie_info['countryMovieInformation']:
            if info['country']['nameEnglish'] == 'KOREA':
                admission_code = info['admissionCode']
                break
        
        if admission_code == '15세이상관람가':
            movie['rating'] = 'OVER15'
        elif admission_code == '12세이상관람가':
            movie['rating'] = 'OVER12'
        elif admission_code == '전체관람가':
            movie['rating'] = 'ALL'
        else:
            movie['rating'] = 'UNKNOWN' 

        movie['titleEnglish'] = title_english
        movie['synopsis'] = plot
        movie['genres'] = genres
        movie['comming'] = 'TRUE'    
        # Remove the 'admissionCode' from the movie dictionary
        movie.pop('admissionCode', None)
        total.append(movie)
    return total


def update_movies_for_collections(**kwargs):
    urls = [
    'https://movie.daum.net/premovie/watcha?flag=Y',
    'https://movie.daum.net/premovie/netflix?flag=Y',
    ]
    for url in urls:
        ti = kwargs['ti']
        movies = fetch_movies_data(url)
        total=update_movies_data(movies)

        if not movies:
            print("영화가 없습니다") # 나중에 slack 메세지 넣어서 확인할 예정
            continue

        # MongoDB에 데이터 적재
        client = pymongo.MongoClient("mongodb://계정:비밀번호@IP:포트번호")
        db = client["db_name"]

        #혹시 없으면 컬렉션 만들기
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


ott_comming_update = PythonOperator(
    task_id='update_movies_for_collections',
    python_callable=update_movies_for_collections,
    provide_context=True,
    dag=dag,
)
gcp_upload = PythonOperator(
    task_id = 'gcp_upload',
    python_callable=gcp_upload,
    provide_context=True,
    dag=dag
)

ott_comming_update >> gcp_upload