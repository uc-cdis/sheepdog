ARG AZLINUX_BASE_VERSION=master

# Base stage with python-build-base
FROM quay.io/cdis/python-build-base:feat_python3.13-alias AS base

ENV appname=sheepdog

WORKDIR /${appname}

RUN chown -R gen3:gen3 /${appname}

# Builder stage
FROM base AS builder

RUN yum install -y \
    gcc \
    python3-devel \
    postgresql-devel \
    libpq-devel && \
    yum clean all

USER gen3

COPY --chown=gen3:gen3 . /${appname}

RUN poetry install -vv --without dev --no-interaction

RUN git config --global --add safe.directory ${appname} && COMMIT=`git rev-parse HEAD` && echo "COMMIT=\"${COMMIT}\"" > ${appname}/version_data.py \
    && VERSION=`git describe --always --tags` && echo "VERSION=\"${VERSION}\"" >> ${appname}/version_data.py

# Final stage
FROM base

# Install runtime dependencies
RUN yum install -y \
    gcc \
    python3-devel \
    postgresql-devel \
    libpq-devel && \
    yum clean all

# Copy poetry artifacts and install the dependencies
COPY poetry.lock pyproject.toml /$appname/
RUN poetry config virtualenvs.create false && \
    poetry install -vv --no-root --without dev --no-interaction && \
    poetry show -v

# Copy application files from the builder stage
COPY --from=builder /${appname} /${appname}

# Switch to non-root user 'gen3' for the serving process
USER gen3

WORKDIR /${appname}

CMD ["/sheepdog/dockerrun.bash"]
