# Example

Requires Docker and PostgreSQL client binaries.

## Run PostgreSQL

```sh
docker run \
  -e POSTGRES_HOST_AUTH_METHOD=trust \
  -e POSTGRES_USER="$USER" \
  -p 5432:5432 \
  --rm \
  postgres
```

Open a new terminal session for the remaining commands.

## Create source database

```sh
PGHOST=localhost createdb source

PGHOST=localhost PGDATABASE=source psql -c '
CREATE TABLE parent (
    id int PRIMARY KEY
);

CREATE TABLE child (
    id int PRIMARY KEY,
    parent_id int REFERENCES parent (id)
);

INSERT INTO parent (id)
VALUES (1), (2);

INSERT INTO child (id, parent_id)
VALUES (1, 1), (2, 1), (3, 2);
'
```

## Dump a slice

```sh
PGHOST=localhost PGDATABASE=source slicedb schema > schema.json
PGHOST=localhost PGDATABASE=source slicedb dump --include-schema --root public.parent 'id = 1' --schema schema.json > slice.zip
```

## Create target database

```sh
PGHOST=localhost createdb target
```

## Restore a slice

```sh
PGHOST=localhost PGDATABASE=target slicedb restore --include-schema < slice.zip
```

## Inspect the result

```sh
PGHOST=localhost PGDATABASE=target psql -c 'TABLE parent' -c 'TABLE child'
```
