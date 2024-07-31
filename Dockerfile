# 기본 이미지
FROM apache/airflow:2.7.2-python3.8

# 루트 사용자로 전환
USER root

# 필수 패키지 및 의존성 설치
RUN apt-get update && \
    apt-get install -y \
    gcc \
    g++ \
    make \
    osmium-tool \
    libosmium2-dev \
    libboost-all-dev \
    libexpat1-dev \
    zlib1g-dev \
    cmake && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# airflow 사용자로 전환
USER airflow

# 작업 디렉토리 설정
WORKDIR /opt/airflow

# 로컬 파일을 컨테이너로 복사
COPY requirements.txt ./
COPY dags/ ./dags/
COPY scripts/ ./scripts/

# Python 패키지 설치
RUN pip install --no-cache-dir -r requirements.txt

# 환경 변수 설정 (필요한 경우)
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV BASE_DIR=/Users/wjdqlscho/PycharmProjects/GEOJSONOSM/logs

# 기본 명령어
CMD ["webserver"]
