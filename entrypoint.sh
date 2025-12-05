#!/bin/sh
set -o errexit
set -o nounset
set -o pipefail

python manage.py migrate --noinput
python manage.py collectstatic --noinput

# Production gunicorn configuration
exec gunicorn config.wsgi:application \
    --bind 0.0.0.0:8000 \
    --workers 4 \
    --worker-class sync \
    --worker-connections 1000 \
    --timeout 60 \
    --keep-alive 5 \
    --max-requests 1000 \
    --max-requests-jitter 50 \
    --access-logfile - \
    --error-logfile - \
    --log-level info
# exec python manage.py runserver 0.0.0.0:8000
