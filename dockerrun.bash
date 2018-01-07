#!/bin/bash

cd /var/www/sheepdog
python wsgi.py
uwsgi --ini /etc/uwsgi/uwsgi.ini &
nginx -g 'daemon off;'
