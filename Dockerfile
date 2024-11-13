ARG AZLINUX_BASE_VERSION=master

# Base stage with python-build-base
FROM quay.io/cdis/python-nginx-al:${AZLINUX_BASE_VERSION} AS base

# FROM 707767160287.dkr.ecr.us-east-1.amazonaws.com/gen3/python-build-base:${AZLINUX_BASE_VERSION} AS base

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

RUN  yum install -y postgresql-libs

COPY --from=builder /${appname} /${appname}

# Switch to non-root user 'gen3' for the serving process
USER gen3

WORKDIR /${appname}

CMD ["/sheepdog/dockerrun.bash"]