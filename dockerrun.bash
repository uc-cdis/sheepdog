#!/bin/bash

nginx
gunicorn -c "/sheepdog/deployment/wsgi/gunicorn.conf.py"