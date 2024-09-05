#!/bin/bash

# nginx
# run gunicorn in the background
echo "starting gunicorn"
gunicorn -c "/sheepdog/deployment/wsgi/gunicorn.conf.py" &
echo "gunicorn submitted"
sleep 30
echo "starting nginx"
nginx -g 'daemon off;'