ARG PG_MAJOR=17
FROM docker.io/library/postgres:${PG_MAJOR}-bookworm
ARG PG_MAJOR

RUN apt-get update -qq \
    && apt-get install -y --no-install-recommends \
       build-essential postgresql-server-dev-${PG_MAJOR} python3-psycopg2 \
       pkg-config libbson-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . /usr/src/documentdb-changefeed
WORKDIR /usr/src/documentdb-changefeed
RUN make -s -j2 && make -s install
