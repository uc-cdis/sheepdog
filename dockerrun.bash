#!/bin/bash

nginx 
poetry run gunicorn -c "/sheepdog/deployment/wsgi/gunicorn.conf.py"
