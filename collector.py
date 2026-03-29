import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

def fetch_and_store_to_supabase():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 데이터 수집 시작...")
    
    # 1. API 데이터 수집
    service_key = os.environ.get('DATA_GO_KR_API_KEY')
    url = 'http://apis.data.go.kr/B552657/ErmctInfoInqireService/getEmrrmRltmUsefulSckbdInfoInqire'
    params = {'serviceKey': service_key, 'STAGE1': '서울특별시', 'pageNo': '1', 'numOfRows': '100'}

    response = requests.get(url, params=params)
    soup = BeautifulSoup(response.content, 'xml')
    items = soup.find_all('item')
    
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
            '수집시각': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        data_list.append(row)

    if not data_list:
        print("❌ 수집된 데이터가 없습니다. API 키나 URL을 확인하세요.")
        return

    df = pd.DataFrame(data_list)
    
    # 2. Supabase DB 연결 및 저장
    # GitHub Secrets에 숨겨둔 주소를 몰래 가져옵니다.
    supabase_db_url = os.environ.get('DATABASE_URL')
    
    if not supabase_db_url:
        print("❌ 에러: DATABASE_URL 환경 변수를 찾을 수 없습니다.")
        return

    try:
        engine = create_engine(supabase_db_url)
        # 테이블 이름을 'er_realtime_log'로 지정하고, 계속 이어서(append) 붙입니다.
        df.to_sql('er_realtime_log', engine, if_exists='append', index=False)
        print(f"✅ Supabase 저장 성공! 총 {len(df)}건 적재 완료.")
    except Exception as e:
        print(f"❌ DB 저장 에러: {e}")

if __name__ == '__main__':
    fetch_and_store_to_supabase()
