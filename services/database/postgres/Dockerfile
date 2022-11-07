FROM postgres:14-alpine AS pg-base

RUN apk add py3-pip make gcc clang libc-dev llvm-dev && \
    pip3 install pgxnclient && \
    pgxn install --yes quantile

COPY load-extensions.sh /docker-entrypoint-initdb.d/
