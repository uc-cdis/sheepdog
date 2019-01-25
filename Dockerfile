# To run: docker run -v /path/to/wsgi.py:/var/www/sheepdog/wsgi.py --name=sheepdog -p 81:80 sheepdog
# To check running container: docker exec -it sheepdog /bin/bash

FROM quay.io/cdis/py27base:feat_python2-base-image

ENV DEBIAN_FRONTEND=noninteractive

RUN mkdir /var/www/sheepdog \
    && mkdir -p /var/www/.cache/Python-Eggs/ \
    && chown www-data -R /var/www/.cache/Python-Eggs/ \
    && mkdir /run/nginx/

COPY . /sheepdog
COPY ./deployment/uwsgi/uwsgi.ini /etc/uwsgi/uwsgi.ini
COPY ./deployment/nginx/nginx.conf /etc/nginx/
COPY ./deployment/nginx/uwsgi.conf /etc/nginx/sites-available/
WORKDIR /sheepdog

RUN python -m pip install -r requirements.txt \
    && COMMIT=`git rev-parse HEAD` && echo "COMMIT=\"${COMMIT}\"" >sheepdog/version_data.py \
    && VERSION=`git describe --always --tags` && echo "VERSION=\"${VERSION}\"" >>sheepdog/version_data.py \ 
    && rm /etc/nginx/conf.d/default.conf \
    && ln -s /etc/nginx/sites-available/uwsgi.conf /etc/nginx/conf.d/uwsgi.conf \
    && ln -sf /dev/stdout /var/log/nginx/access.log \
    && ln -sf /dev/stderr /var/log/nginx/error.log \
    && chown www-data /var/www/sheepdog

EXPOSE 80

WORKDIR /var/www/sheepdog

CMD /sheepdog/dockerrun.bash
