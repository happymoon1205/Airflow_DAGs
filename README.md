## 과정
1. CGV, LOTTE, OTT들의 개봉예정작들을 크롤링합니다
2. mongoDB에 중복을 검사하고 없다면 적재합니다
3. 적재한 영화들을 다시 mongoDB에 검색하여 포스터를 구글 스토리지에 {_id}.jpg로 적재합니다

>DAG들이 같은 mongoDB의 collection에 적재되기때문에 시간의 차이를 두고 실행을 했습니다.

# DAG들의 설명

## cgv_coming.py

이 DAG는 CGV 웹사이트에서 개봉 예정인 영화 데이터를 가져와 MongoDB에 저장하고 Google Cloud Storage에 영화 포스터를 업로드합니다.

### Tasks

* `scrape_cgv_movies`: CGV 웹사이트에서 개봉 예정인 영화 데이터를 가져와 사전 정의된 필드로 구성된 리스트로 반환합니다.
* `store_to_mongodb`: MongoDB에 영화 데이터를 저장합니다.
* `gcp_upload`: Google Cloud Storage에 영화 포스터를 업로드합니다.

### Schedule

이 DAG는 매일 오전 1시에 실행됩니다.

## lotte_movie_scraper.py

이 DAG는 롯데 시네마 웹사이트에서 영화 데이터를 가져와 MongoDB에 저장하고 Google Cloud Storage에 영화 포스터를 업로드합니다.

### Tasks

* `scrape_lotte_movies`: 롯데 시네마 웹사이트에서 영화 데이터를 가져와 사전 정의된 필드로 구성된 리스트로 반환합니다.
* `store_to_mongodb`: MongoDB에 영화 데이터를 저장합니다.
* `gcp_upload`: Google Cloud Storage에 영화 포스터를 업로드합니다.

### Schedule

이 DAG는 매일 오전 1시 15분에 실행됩니다.

## ott_comming_update.py

이 DAG는 다음 OTT 플랫폼에서 개봉 예정인 영화 데이터를 가져와 MongoDB에 업데이트합니다.

* Watcha
* Netflix

### Tasks

* `update_movies_for_collections`: 다음 OTT 플랫폼의 개봉 예정인 영화 데이터를 가져와 MongoDB에 업데이트합니다.
* `gcp_upload`: MongoDB에 업데이트된 영화 데이터를 기반으로 Google Cloud Storage에 영화 포스터를 업로드합니다.

### Schedule

이 DAG는 매일 오전 1시 30분에 실행됩니다.

