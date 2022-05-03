https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html

    mkdir -p ./dags ./logs ./plugins
    echo -e "AIRFLOW_UID=$(id -u)" > .env

    docker-compose up airflow-init

to access the webserver container and use airflow cli use

    docker container exec -it airflow-airflow-webserver-1 /bin/bash

build the extending image with

    docker build . --tag extending_airflow:latest

this will build a proper docker image with the requeriments defined for your dag on requirements.txt
do not forget to replace line 47 with the following

      image: ${AIRFLOW_IMAGE_NAME:-extending_airflow:latest}