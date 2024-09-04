#!/bin/bash

# nginx
gunicorn -c "/sheepdog/deployment/wsgi/gunicorn.conf.py" &
echo "gunicorn submitted"
sleep 60
echo "starting nginx"
nginx