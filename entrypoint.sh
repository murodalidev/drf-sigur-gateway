#!/bin/sh
set -o errexit
set -o nounset
set -o pipefail

python manage.py migrate --noinput
python manage.py collectstatic --noinput

exec gunicorn config.wsgi:application --bind 0.0.0.0:8000
# exec python manage.py runserver 0.0.0.0:8000
