FROM apache/airflow:3.3.2

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt "SQLAlchemy<2.1" "pathspec<1.1"