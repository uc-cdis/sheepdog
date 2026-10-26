#!/bin/bash

nginx
exec gunicorn -c "/sheepdog/deployment/wsgi/gunicorn.conf.py"
