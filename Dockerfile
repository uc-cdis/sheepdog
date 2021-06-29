# To run: docker run -v /path/to/wsgi.py:/var/www/sheepdog/wsgi.py --name=sheepdog -p 81:80 sheepdog
# To check running container: docker exec -it sheepdog /bin/bash

FROM quay.io/cdis/python-nginx:pybase3-1.6.0

RUN apk update \
    && apk add postgresql-libs postgresql-dev libffi-dev libressl-dev \
    && apk add linux-headers musl-dev gcc libxml2-dev libxslt-dev \
    && apk add curl bash git vim

COPY . /sheepdog
COPY ./deployment/uwsgi/uwsgi.ini /etc/uwsgi/uwsgi.ini
WORKDIR /sheepdog
ENV CRYPTOGRAPHY_DONT_BUILD_RUST=1

# TODO: consider pinned version of pip.
# hadolint ignore=DL3013
RUN python -m pip install --upgrade pip \
    && python -m pip install --upgrade setuptools \
    && python /sheepdog/setup.py install \
    && pip --version \
    && pip install -r requirements.txt

RUN mkdir -p /var/www/sheepdog \
    && mkdir /run/ngnix/ \
    && chown nginx /var/www/sheepdog

EXPOSE 80

# TODO: Check using legacy notation instead of backticked
# hadolint ignore=SC2006
RUN COMMIT=`git rev-parse HEAD` && echo "COMMIT=\"${COMMIT}\"" >sheepdog/version_data.py \
    && VERSION=`git describe --always --tags` && echo "VERSION=\"${VERSION}\"" >>sheepdog/version_data.py \
    && python setup.py install

WORKDIR /var/www/sheepdog

CMD /dockerrun.sh
