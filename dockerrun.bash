#!/bin/bash

poetry run gunicorn -c "/sheepdog/deployment/wsgi/gunicorn.conf.py" &
# sleep to prevent nginx warning logs so nginx doesn't start before the app
sleep 30
nginx -g 'daemon off;'