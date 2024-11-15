#!/bin/bash

poetry run gunicorn -c "/sheepdog/deployment/wsgi/gunicorn.conf.py" 
nginx