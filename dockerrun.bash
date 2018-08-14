#!/bin/bash

cd /var/www/sheepdog

export PYTHONUNBUFFERED=TRUE
#
# Update certificate authority index -
# environment may have mounted more authorities
# - ex: /usr/local/share/ca-certificates/cdis-ca.crt into system bundle       
#
update-ca-certificates
#
# Enable debug flag based on GEN3_DEBUG environment
#
if [ -f ./wsgi.py && "$GEN3_DEBUG" == "True" ]; then
  echo -e "\napplication.debug=True\n" >> ./wsgi.py
fi  

(
  # Wait for nginx to create uwsgi.sock
  let count=0
  while [[ (! -e uwsgi.sock) && count -lt 10 ]]; do
    sleep 2
    let count="$count+1"
  done
  if [[ ! -e uwsgi.sock ]]; then
    echo "WARNING: /var/www/sheepdog/uwsgi.sock does not exist!!!"
  fi
  uwsgi --ini /etc/uwsgi/uwsgi.ini
) &

nginx -g 'daemon off;'
