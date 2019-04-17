# To run: docker run -v /path/to/wsgi.py:/var/www/sheepdog/wsgi.py --name=sheepdog -p 81:80 sheepdog
# To check running container: docker exec -it sheepdog /bin/bash 

FROM quay.io/cdis/py27base:pybase2-1.0.1

ENV DEBIAN_FRONTEND=noninteractive

RUN mkdir /var/www/sheepdog \
    && chown www-data /var/www/sheepdog

COPY . /sheepdog
COPY ./deployment/uwsgi/uwsgi.ini /etc/uwsgi/uwsgi.ini
WORKDIR /sheepdog

RUN python -m pip install -r requirements.txt \
    && COMMIT=`git rev-parse HEAD` && echo "COMMIT=\"${COMMIT}\"" >sheepdog/version_data.py \
    && VERSION=`git describe --always --tags` && echo "VERSION=\"${VERSION}\"" >>sheepdog/version_data.py 

EXPOSE 80 

WORKDIR /var/www/sheepdog

CMD /dockerrun.sh
