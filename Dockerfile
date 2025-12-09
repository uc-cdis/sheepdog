ARG AZLINUX_BASE_VERSION=master

# Base stage with python-build-base
FROM quay.io/cdis/amazonlinux-base:3.13-pythonbase AS base

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

# Switch to non-root user 'gen3' for the serving process
USER gen3

WORKDIR /${appname}

CMD ["/sheepdog/dockerrun.bash"]
