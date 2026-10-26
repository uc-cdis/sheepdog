ARG AZLINUX_BASE_VERSION=master

# Base stage with python-build-base
FROM quay.io/cdis/amazonlinux-base:3.13-pythonnginx AS base

ENV appname=sheepdog

WORKDIR /${appname}

RUN chown -R gen3:gen3 /${appname}

# Builder stage
FROM base AS builder

USER root

RUN yum install -y \
    gcc \
    python3-devel \
    postgresql-devel \
    libpq-devel && \
    yum clean all

COPY --chown=gen3:gen3 . /${appname}

RUN poetry install -vv --without dev --no-interaction

USER gen3

RUN git config --global --add safe.directory ${appname} && COMMIT=`git rev-parse HEAD` && echo "COMMIT=\"${COMMIT}\"" > ${appname}/version_data.py \
    && VERSION=`git describe --always --tags` && echo "VERSION=\"${VERSION}\"" >> ${appname}/version_data.py

# Final stage
FROM base

USER root 

# Install runtime dependencies
RUN yum install -y \
    gcc \
    python3-devel \
    postgresql-devel \
    libpq-devel && \
    yum clean all

# Copy poetry artifacts and install the dependencies
COPY poetry.lock pyproject.toml /$appname/
RUN poetry config virtualenvs.create false 
RUN poetry install -vv --no-root --without dev --no-interaction 
RUN poetry show -v

# Copy application files from the builder stage
COPY --from=builder /${appname} /${appname}

# OpenShift runs containers with an arbitrary UID in the root group (GID 0),
# so everything the process writes to must be group-writable by GID 0.
# nginx listens on 8080 (unprivileged) using a config without the `user` directive.
COPY deployment/nginx/nginx.conf /etc/nginx/nginx.conf
# Drop nginx's file capability (cap_net_bind_service); restricted SCCs drop all caps
# and the binary would otherwise fail to exec. Not needed on port 8080.
RUN setcap -r /usr/sbin/nginx && \
    mkdir -p /tmp/nginx && \
    chgrp -R 0 /${appname} /var/lib/nginx /var/log/nginx /tmp/nginx && \
    chmod -R g=u /${appname} /var/lib/nginx /var/log/nginx /tmp/nginx && \
    chmod g+rx /etc/nginx/nginx.conf

# Arbitrary UIDs have no home dir; gunicorn writes its control socket under $HOME
ENV HOME=/tmp

EXPOSE 8080

# Non-root user; OpenShift will override the UID but keep GID 0
USER 1000:0

WORKDIR /${appname}

CMD ["/sheepdog/dockerrun.bash"]
