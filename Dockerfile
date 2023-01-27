# To run:
# - Create and fill out `creds.json`:
# {
#   "fence_host": "",
#   "fence_username": "",
#   "fence_password": "",
#   "fence_database": "",
#   "db_host": "",
#   "db_username": "",
#   "db_password": "",
#   "db_database": "",
#   "gdcapi_secret_key": "",
#   "indexd_password": "",
#   "hostname": "",
#   "oauth2_client_id": "",
#   "oauth2_client_secret": ""
# }
# - Build the image: `docker build . -t sheepdog -f Dockerfile`
# - Run: `docker run -v /full/path/to/creds.json:/var/www/sheepdog/creds.json -p 81:80 sheepdog`
# To check running container: `docker exec -it sheepdog /bin/bash`

FROM quay.io/cdis/python:python3.9-buster-2.0.0

ENV appname=sheepdog

RUN pip install --upgrade pip poetry
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libffi-dev musl-dev gcc libxml2-dev libxslt-dev \
    curl bash git vim

RUN mkdir -p /var/www/$appname \
    && mkdir -p /var/www/.cache/Python-Eggs/ \
    && mkdir /run/nginx/ \
    && ln -sf /dev/stdout /var/log/nginx/access.log \
    && ln -sf /dev/stderr /var/log/nginx/error.log \
    && chown nginx -R /var/www/.cache/Python-Eggs/ \
    && chown nginx /var/www/$appname

EXPOSE 80

WORKDIR /$appname

# copy ONLY poetry artifact, install the dependencies but not indexd
# this will make sure than the dependencies is cached
COPY poetry.lock pyproject.toml /$appname/
RUN poetry config virtualenvs.create false \
    && poetry install -vv --no-root --no-dev --no-interaction \
    && poetry show -v

# copy source code ONLY after installing dependencies
COPY . /$appname
COPY ./deployment/uwsgi/uwsgi.ini /etc/uwsgi/uwsgi.ini
COPY ./bin/settings.py /var/www/sheepdog/settings.py
COPY ./bin/confighelper.py /var/www/sheepdog/confighelper.py

# install indexd
RUN poetry config virtualenvs.create false \
    && poetry install -vv --no-dev --no-interaction \
    && poetry show -v

RUN COMMIT=`git rev-parse HEAD` && echo "COMMIT=\"${COMMIT}\"" >$appname/version_data.py \
    && VERSION=`git describe --always --tags` && echo "VERSION=\"${VERSION}\"" >>$appname/version_data.py

WORKDIR /var/www/$appname

RUN ls
CMD /dockerrun.sh
