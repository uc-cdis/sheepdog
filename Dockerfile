# To run: docker run -v /path/to/wsgi.py:/var/www/sheepdog/wsgi.py --name=sheepdog -p 81:80 sheepdog
# To check running container: docker exec -it sheepdog /bin/bash

FROM ubuntu:16.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    # dependency for cryptography
    libffi-dev \
    # dependency for pyscopg2 - which is dependency for sqlalchemy postgres engine
    libpq-dev \
    # dependency for cryptography
    libssl-dev \
    libxml2-dev \
    libxslt1-dev \
    nginx \
    python2.7 \
    python-dev \
    python-pip \
    python-setuptools \
    sudo \
    vim \
    && pip install --upgrade pip \
    && pip install --upgrade setuptools \
    && pip install uwsgi \
    && mkdir /var/www/sheepdog \
    && mkdir -p /var/www/.cache/Python-Eggs/ \
    && chown www-data -R /var/www/.cache/Python-Eggs/ \
    && mkdir /run/nginx/

COPY . /sheepdog
COPY ./deployment/uwsgi/uwsgi.ini /etc/uwsgi/uwsgi.ini
COPY ./deployment/nginx/uwsgi.conf /etc/nginx/sites-available/
WORKDIR /sheepdog

RUN pip install -r requirements.txt \
    && COMMIT=`git rev-parse HEAD` && echo "COMMIT=\"${COMMIT}\"" >sheepdog/version_data.py \
    && VERSION=`git describe --always --tags` && echo "VERSION=\"${VERSION}\"" >>sheepdog/version_data.py \
    && DICTCOMMIT=`git rev-parse HEAD` && echo "DICTCOMMIT=\"${DICTCOMMIT}\"" >>/sheepdog/sheepdog/version_data.py \
    && DICTVERSION=`git describe --always --tags` && echo "DICTVERSION=\"${DICTVERSION}\"" >>/sheepdog/sheepdog/version_data.py \
    && rm /etc/nginx/sites-enabled/default \
    && ln -s /etc/nginx/sites-available/uwsgi.conf /etc/nginx/sites-enabled/uwsgi.conf \
    && ln -sf /dev/stdout /var/log/nginx/access.log \
    && ln -sf /dev/stderr /var/log/nginx/error.log \
    && chown www-data /var/www/sheepdog

EXPOSE 80

WORKDIR /var/www/sheepdog

CMD /sheepdog/dockerrun.bash
