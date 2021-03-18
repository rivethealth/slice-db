FROM python:3.9-slim-buster

RUN apt-get update \
  && apt-get install -y curl gnupg2 \
  && rm -rf /var/lib/apt/lists/*

RUN echo "deb http://apt.postgresql.org/pub/repos/apt buster-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
  && curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -

RUN apt-get update \
  && apt-get install -y postgresql-client-13 \
  && rm -rf /var/lib/apt/lists/*

COPY . /tmp/slice-db

RUN pip install --no-cache-dir /tmp/slice-db

ENTRYPOINT /usr/local/bin/slice-db
