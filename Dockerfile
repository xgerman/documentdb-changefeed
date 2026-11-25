ARG PG_MAJOR=17
FROM docker.io/library/postgres:${PG_MAJOR}-bookworm
ARG PG_MAJOR

RUN apt-get update -qq \
    && apt-get install -y --no-install-recommends \
       build-essential postgresql-server-dev-${PG_MAJOR} python3-psycopg2 \
       pkg-config libbson-dev curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/documentdb-source \
    && curl -fsSL --connect-timeout 20 --max-time 180 \
       https://codeload.github.com/documentdb/documentdb/tar.gz/d9301892d7fa47b6f5bb923c93b1f6915c3b9bad \
       -o /tmp/documentdb-headers.tar.gz \
    && tar -xzf /tmp/documentdb-headers.tar.gz --strip-components=1 -C /opt/documentdb-source \
    && rm /tmp/documentdb-headers.tar.gz

COPY . /usr/src/documentdb-changefeed
WORKDIR /usr/src/documentdb-changefeed
RUN make -s -j2 && make -s install
