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

For the remaining steps, open a new terminal session and run

```sh
export PGHOST=localhost
```

## Create source database

```sh
createdb source

PGDATABASE=source psql -c '
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
PGDATABASE=source slicedb schema > schema.json
PGDATABASE=source slicedb dump --include-schema --root public.parent 'id = 1' --schema schema.json > slice.zip
```

## Create target database

```sh
createdb target
```

## Restore a slice

```sh
PGDATABASE=target slicedb restore --include-schema < slice.zip
```

## Inspect the result

```sh
PGDATABASE=target psql -c 'TABLE parent' -c 'TABLE child'
```

```
 id
----
  1
(1 row)

 id | parent_id
----+-----------
  1 |         1
  2 |         1
(2 rows)
```
