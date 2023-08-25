from datetime import datetime, timedelta
import requests
import json
from bson import json_util
import pymongo
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
import os
from io import BytesIO
import logging


default_args = {
    'owner': 'airflow',                  # DAG의 소유자
    'depends_on_past': False,            # 이전 실행이 성공하든 실패하든 실행(이전 실행에 대한 의존여부 지정)
    'start_date': datetime(2023, 8, 1),  # 시작날짜
    'retries': 1,                        # 실패시 재시도 횟수
    'retry_delay': timedelta(minutes=5), # 실패시 다음시도까지 대기 시간
}

dag= DAG(
    'lotte_movie_scraper',                 # DAG이름
    default_args=default_args,
    description='Scrape Lotte movie data', # DAG설명
    schedule_interval='15 1 * * *',        # 시작시간
) 

def scrape_lotte_movies():
    url ='https://www.lottecinema.co.kr/LCWS/Movie/MovieData.aspx'
    pay={"MethodName":"GetMoviesToBe","channelType":"HO","osType":"Chrome","osVersion":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36","multiLanguageID":"KR","division":1,"moviePlayYN":"N","orderType":"5","blockSize":100,"pageNo":1,"memberOnNo":""}
    data = {"paramList": str(pay).encode()}
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    r = requests.post(url, data=data, headers=headers).text
    
    lottemovie=json.loads(r)['Movies']['Items']
    mlist=[i['RepresentationMovieCode'] for i in lottemovie]
    # 영화의 코드만 가져온다
    cnt=0
    for i in mlist: 
        if i == 'AD':
            del mlist[cnt]
        cnt+=1
    total=[]
    for a in mlist:
        direc=[]
        actor=[]
        lotteDic={}
        url='https://www.lottecinema.co.kr/LCWS/Movie/MovieData.aspx'
        pay={"MethodName":"GetMovieDetailTOBE","channelType":"HO","osType":"Chrome","osVersion":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36","multiLanguageID":"KR","representationMovieCode":a,"memberOnNo":""}
        data={"paramList":str(pay).encode()}
        r=requests.post(url,data=data)
        info=r.json()['Movie']
        casting=r.json()['Casting']['Items']
        # 제목 한글
        lotteDic['title_kr'] = info['MovieNameKR']  # 제목

        #배우, 감독
        for i in casting:
            if isinstance(i, dict) and 'Role' in i and 'StaffName' in i:
                role = i['Role'].strip()
                if '감독' in role:  # 감독
                    direc.append(i['StaffName'])
                if '배우' in role:  # 배우
                    actor.append(i['StaffName'])
        lotteDic['direc']=direc #감독
        lotteDic['actor']=actor #배우    
        #나라
        lotteDic['nations']=info['MakingNationNameKR']

        #개봉일자
        released_at_str = info['ReleaseDate']
        released_at_date = released_at_str.split(' ')[0]    
        lotteDic['released_At'] = released_at_date
        # 장르
        genre = [] 
        for key, value in info.items():
            if 'MovieGenreNameKR' in key and not None and value!='':
                genre.append(value)
        lotteDic['genres']=genre
        #줄거리
        if info['SynopsisKR'] is not None:
            lotteDic['synopsis'] = info['SynopsisKR'].replace('<b>', '').replace('</b>', '').replace('<br>', '')
        else:
            lotteDic['synopsis'] = ""

        #포스터 링크
        lotteDic['poster_img_url']=info['PosterURL']
   
        # 등급
        view_grade_info = info['ViewGradeNameUS']
        view_grade = view_grade_info.split(',')[0].strip()
        if view_grade == '전체관람가':
            lotteDic['rating'] = 'ALL'
        elif view_grade == '만12세이상관람가':
            lotteDic['rating'] = 'OVER12'
        elif view_grade == '만15세이상관람가':
            lotteDic['rating'] = 'OVER15'
        elif view_grade == '만18세이상관람가':
            lotteDic['rating'] = 'OVER18'
        else:
            lotteDic['rating'] = 'UNKNOWN'
        #OTT 정보
        lotteDic['OTT']='lotte'

        total.append(lotteDic)
    return total

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
    


scrape_lotte_movies_task = PythonOperator(
    task_id='scrape_lotte_movies',
    python_callable=scrape_lotte_movies,
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

scrape_lotte_movies_task >> store_to_mongodb_task >> gcp_upload