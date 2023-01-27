#!/bin/bash
export AIRFLOW_HOME=/home/24859/airflow-cs280
cd /home/24859/airflow-cs280
source /home/24859/miniconda3/bin/activate
conda activate /home/24859/miniconda3/envs/airflow-env
nohup airflow scheduler >> scheduler.log &
nohup airflow webserver -p 8080 >> webserver.log &
