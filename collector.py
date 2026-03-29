import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine

def get_kst_now():
    # 한국 시간(UTC+9) 계산
    return datetime.now() + timedelta(hours=9)

def fetch_and_store_to_supabase():
    kst_now = get_kst_now()
    print(f"[{kst_now.strftime('%Y-%m-%d %H:%M:%S')}] 데이터 수집 시작...")
    
    # 1. 환경 변수에서 키 가져오기
    service_key = os.environ.get('DATA_GO_KR_API_KEY')
    supabase_db_url = os.environ.get('DATABASE_URL')

    if not service_key or not supabase_db_url:
        print("❌ 에러: GitHub Secrets에서 API 키 또는 DB 주소를 불러오지 못했습니다.")
        return

    url = 'http://apis.data.go.kr/B552657/ErmctInfoInqireService/getEmrrmRltmUsefulSckbdInfoInqire'
    params = {'serviceKey': service_key, 'pageNo': '1', 'numOfRows': '1000'}

    try:
        # API 호출
        response = requests.get(url, params=params)
        
        # 🌟 중요: API가 실제로 뭐라고 대답하는지 로그에 찍습니다 (에러 확인용)
        print(f"DEBUG: API 응답 코드: {response.status_code}")
        print(f"DEBUG: API 응답 내용 요약: {response.text[:500]}")

        soup = BeautifulSoup(response.content, 'xml')
        items = soup.find_all('item')
        
        if not items:
            print("❌ 수집된 데이터가 없습니다. API 응답 내용(DEBUG)을 확인하세요.")
            return

        data_list = []
        for item in items:
            row = {
                '기관코드': item.find('hpid').text if item.find('hpid') else '',
                '기관명': item.find('dutyName').text if item.find('dutyName') else '',
                '일반응급실병상_hvec': item.find('hvec').text if item.find('hvec') else '',
                '수술실_hvoc': item.find('hvoc').text if item.find('hvoc') else '',
                'CT가용': item.find('hvctayn').text if item.find('hvctayn') else '',
                'MRI가용': item.find('hvmriayn').text if item.find('hvmriayn') else '',
                '신경중환자실': item.find('hvcc').text if item.find('hvcc') else '',
                '신경외과중환자실': item.find('hv6').text if item.find('hv6') else '',
                '조영촬영기': item.find('hvangioayn').text if item.find('hvangioayn') else '',
                '흉부중환자실': item.find('hvccc').text if item.find('hvccc') else '',
                '인공호흡기': item.find('hvventiayn').text if item.find('hvventiayn') else '',
                '외상중환자실': item.find('hv9').text if item.find('hv9') else '',
                '업데이트시각': item.find('hvidate').text if item.find('hvidate') else '',
                '수집시각': kst_now.strftime('%Y-%m-%d %H:%M:%S')
            }
            data_list.append(row)

        df = pd.DataFrame(data_list)
        
        # 2. DB 저장
        engine = create_engine(supabase_db_url)
        df.to_sql('er_realtime_log', engine, if_exists='append', index=False)
        print(f"✅ Supabase 저장 성공! ({len(df)}건 적재 완료)")

    except Exception as e:
        print(f"❌ 실행 중 에러 발생: {e}")

if __name__ == '__main__':
    fetch_and_store_to_supabase()
