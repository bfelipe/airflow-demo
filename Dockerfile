FROM apache/airflow:2.2.4

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt
