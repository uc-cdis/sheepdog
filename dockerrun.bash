#!/bin/bash

gunicorn -c "/sheepdog/deployment/wsgi/gunicorn.conf.py" &
sleep 30
nginx -g 'daemon off;'