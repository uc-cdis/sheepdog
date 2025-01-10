ARG AZLINUX_BASE_VERSION=master

# Base stage with python-build-base
FROM quay.io/cdis/python-nginx-al:${AZLINUX_BASE_VERSION} AS base

ENV appname=sheepdog

WORKDIR /${appname}

RUN chown -R gen3:gen3 /${appname}

# Builder stage
FROM base AS builder

RUN dnf install -y python3-devel postgresql-devel gcc

USER gen3

COPY poetry.lock pyproject.toml /${appname}/

RUN poetry install -vv --without dev --no-interaction

COPY --chown=gen3:gen3 . /${appname}

# Run poetry again so this app itself gets installed too
RUN poetry install --without dev --no-interaction

RUN git config --global --add safe.directory /${appname} && COMMIT=`git rev-parse HEAD` && echo "COMMIT=\"${COMMIT}\"" > /${appname}/version_data.py \
    && VERSION=`git describe --always --tags` && echo "VERSION=\"${VERSION}\"" >> /${appname}/version_data.py

# Final stage
FROM base

# Copy poetry artifacts and install the dependencies
# This will ensure dependencies are cached
COPY poetry.lock pyproject.toml /$appname/
RUN poetry config virtualenvs.create false \
    && poetry install -vv --no-root --without dev --no-interaction \
    && poetry show -v

# Install PostgreSQL libraries
RUN yum install -y postgresql-libs

# Copy application files from the builder stage
COPY --from=builder /${appname} /${appname}

# Install sheepdog
RUN poetry config virtualenvs.create false \
    && poetry install -vv --without dev --no-interaction \
    && poetry show -v

# Switch to non-root user 'gen3' for the serving process
USER gen3

WORKDIR /${appname}

CMD ["/sheepdog/dockerrun.bash"]
