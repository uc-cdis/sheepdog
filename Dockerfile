ARG AZLINUX_BASE_VERSION=master

# Base stage with python-build-base
FROM quay.io/cdis/python-nginx-al:${AZLINUX_BASE_VERSION} AS base

ENV appname=sheepdog

WORKDIR /${appname}

RUN chown -R gen3:gen3 /${appname}

# Builder stage
FROM base AS builder

RUN dnf install -y python3-devel postgresql-devel gcc libpq-devel

USER gen3

COPY --chown=gen3:gen3 . /${appname}

RUN poetry install -vv --without dev --no-interaction

COPY --chown=gen3:gen3 . /${appname}

RUN git config --global --add safe.directory /${appname} && COMMIT=`git rev-parse HEAD` && echo "COMMIT=\"${COMMIT}\"" > /${appname}/version_data.py \
    && VERSION=`git describe --always --tags` && echo "VERSION=\"${VERSION}\"" >> /${appname}/version_data.py

# Final stage
FROM base

# Copy poetry artifacts and install the dependencies
# This will ensure dependencies are cached
COPY poetry.lock pyproject.toml /$appname/
RUN dnf install -y postgresql-devel gcc && \
    poetry config virtualenvs.create false && \
    poetry install -vv --no-root --without dev --no-interaction && \
    poetry show -v

# Install PostgreSQL libraries
RUN dnf install -y python3-devel postgresql-devel gcc libpq-devel

# Copy application files from the builder stage
COPY --from=builder /${appname} /${appname}

# Switch to non-root user 'gen3' for the serving process
USER gen3

WORKDIR /${appname}

CMD ["/sheepdog/dockerrun.bash"]
