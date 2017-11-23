#!/bin/bash
echo "Starting the Azizi AMP Application!"

echo "Wait for DB server to be ready"
/opt/scripts/waitforit.sh "${MYSQL_HOST}:3306"

python /opt/azizi_amp/manage.py migrate
python /opt/azizi_amp/manage.py migrate --run-syncdb 
python /opt/azizi_amp/manage.py runserver 0.0.0.0:8089