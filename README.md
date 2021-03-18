# SliceDB

[![PyPI](https://img.shields.io/pypi/v/slice-db)](https://pypi.org/project/slice-db/)

## Overview

SliceDB is a tool for capturing and restoring a subset of a PostgreSQL database.
It also supports scrubbing sensive data.

## Install

### Pip

```sh
pip3 install psycopg2-binary slice_db
```

### Docker

```sh
docker pull rivethealth/slicedb
```

## Usage

For all commands and options, see [Usage](doc/usage.md).

## Basic example

First, query a database to create a schema file.

```sh
slicedb schema > schema.yml
```

Second, dump a slice:

```sh
slicedb dump --root public.example 'WHERE id IN (7, 56, 234)' --schema schema.yml > slice.zip
```

Third, restore that slice into another database:

```sh
slicedb restore < slice.zip
```

For a complete working example, see [Example](doc/example.md).

## Connection

Use the
[libpq environment variables](https://www.postgresql.org/docs/current/libpq-envars.html)
to configure the connection.

```sh
PGHOST=myhost slicedb schema > slice.yml
```

## Dump

### Output types

SliceDB can produce multiple formats:

- **slice** - ZIP archive of table data. This can be restored into an existing
  database with `slicedb restore`.
- **sql** - SQL file. This can be restored into an existing database with `psql`
  or another client, if triggers are disabled. It may include the schema, in
  which case it is restored into a new database.

### Schema

See formats/schema.yml for the JSONSchema of the schema file.

The `schema` command uses foreign keys to infer relationships between tables. It
is a suggested starting point.

You may want to prune the slice by removing relationships, or expand the slice
by adding relationships that don't have explicit foreign keys.

`slicedb schema-filter` can help modify the schema, or generic JSON tools like
`jq`.

### Algorithm

The slicing process works as follows:

1. Starting with the root table, query the physical IDs (ctid) of rows.

2. Add the row IDs to the existing list.

3. For new IDs, process each of the adjacent tables, using them as the current
   root.

Do this in parallel, using `pg_export_snapshot()` to guarantee a consistent
snapshot across workers.

### Performance

Hundreds of thousands of rows can be exported in only a few minutes and several
dozen MBs of memory.

## Transformation

_TODO_

Replacements are deterministic for a given pepper. By default, the pepper is
randomly geneated for a slice. You may specify it as `--pepper`. Note that
possession of the pepper makes the data guessable.

Transformation may operate an existing slice, or happen during the dump.

### Replacments

- `alphanumeric` - Replace alphanumeric characters, preserve the type and case
  of characters.
- `date_year` - Change date by up to one year.
- `geozip` - Replace zip code, preserving the first three digits.
- `given_name` - Replace given name.
- `person_name` - Replace name.
- `surname` - Replace surname.
- `composite` - Parse as a PostgreSQL composite, with suboptions.

### Replacement data

- Given names:
  [https://www.ssa.gov/cgi-bin/popularnames.cgi](https://www.ssa.gov/cgi-bin/popularnames.cgi)
- Surnames:
  [https://raw.githubusercontent.com/fivethirtyeight/data/master/most-common-name/surnames.csv](https://raw.githubusercontent.com/fivethirtyeight/data/master/most-common-name/surnames.csv)
- Zip codes:
  [https://simplemaps.com/data/us-zips](https://simplemaps.com/data/us-zips)

## Restore

SliceDB can restore slices into existing databases. In practice, this should
normally be an empty existing database.

### Cycles

Foreign keys may form a cycle only if at least one foreign key in the cycle is
deferrable.

That foreign key will be deferred during restore.

A restore may happen in a single transaction or not. Parallelism requires
multiple transactions.

## Not supported

- Multiple databases
- Databases other than PostgreSQL
