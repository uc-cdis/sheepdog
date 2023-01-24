# To run: docker run -v /path/to/wsgi.py:/var/www/sheepdog/wsgi.py --name=sheepdog -p 81:80 sheepdog
# To check running container: docker exec -it sheepdog /bin/bash

FROM quay.io/cdis/python:python3.9-buster-2.0.0

ENV appname=sheepdog

RUN pip install --upgrade pip poetry
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libffi-dev musl-dev gcc libxml2-dev libxslt-dev \
    curl bash git vim

# COPY . /sheepdog
# COPY ./deployment/uwsgi/uwsgi.ini /etc/uwsgi/uwsgi.ini
# WORKDIR /sheepdog
# ENV CRYPTOGRAPHY_DONT_BUILD_RUST=1

# TODO: consider pinned version of pip.
# hadolint ignore=DL3013
RUN python3 -m pip install --upgrade pip \
    && python3 -m pip install --upgrade setuptools
    # && python3 /sheepdog/setup.py install \
    # && python3 -m pip --version \
    # && python3 -m pip install -r requirements.txt

# RUN mkdir -p /var/www/sheepdog \
#     && mkdir /run/ngnix/ \
#     && chown nginx /var/www/sheepdog

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
COPY ./deployment/uwsgi/wsgi.py /$appname/wsgi.py
COPY clear_prometheus_multiproc /$appname/clear_prometheus_multiproc

# install indexd
RUN poetry config virtualenvs.create false \
    && poetry install -vv --no-dev --no-interaction \
    && poetry show -v

RUN COMMIT=`git rev-parse HEAD` && echo "COMMIT=\"${COMMIT}\"" >$appname/index/version_data.py \
    && VERSION=`git describe --always --tags` && echo "VERSION=\"${VERSION}\"" >>$appname/index/version_data.py

WORKDIR /var/www/$appname

CMD /dockerrun.sh

# # TODO: Check using legacy notation instead of backticked
# # hadolint ignore=SC2006
# RUN COMMIT=`git rev-parse HEAD` && echo "COMMIT=\"${COMMIT}\"" >sheepdog/version_data.py \
#     && VERSION=`git describe --always --tags` && echo "VERSION=\"${VERSION}\"" >>sheepdog/version_data.py \
#     && python3 setup.py install

# WORKDIR /var/www/sheepdog

# CMD /dockerrun.sh
