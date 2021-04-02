def query_schema(cur):
    """
    Query PostgreSQL for schema
    """
    cur.execute(
        """
            SELECT
                json_build_object(
                    'references', pc2.references,
                    'tables', pc.tables
                )
            FROM
                (
                    SELECT
                        coalesce(
                            json_object_agg(
                                pn.nspname || '.' || pc.relname,
                                json_build_object(
                                    'columns', pa.columns,
                                    'name', pc.relname,
                                    'schema', pn.nspname
                                )
                            ),
                            '[]'
                        ) AS tables
                    FROM
                        pg_class AS pc
                        JOIN pg_namespace AS pn ON pc.relnamespace = pn.oid
                        CROSS JOIN LATERAL (
                            SELECT coalesce(json_agg(pa.attname ORDER BY pa.attnum), '[]') AS columns
                            FROM pg_attribute AS pa
                            WHERE pc.oid = pa.attrelid AND 0 < pa.attnum AND NOT pa.attisdropped
                        ) AS pa
                    WHERE
                        pn.nspname <> 'information_schema'
                        AND pn.nspname NOT LIKE 'pg_%'
                        AND pc.relkind = 'r'
                ) AS pc
                CROSS JOIN (
                    SELECT
                        coalesce(
                            json_object_agg(
                                pn.nspname || '.' || pc2.relname || '.' || pc.conname,
                                json_build_object(
                                    'columns', pa.columns,
                                    'referenceColumns', pa.reference_columns,
                                    'referenceTable', pn2.nspname || '.' || pc3.relname,
                                    'table', pn.nspname || '.' || pc2.relname
                                )
                            ),
                            '{}'
                        ) AS references
                    FROM
                        pg_constraint AS pc
                        JOIN pg_class AS pc2 ON pc.conrelid = pc2.oid
                        JOIN pg_namespace AS pn ON pc2.relnamespace = pn.oid
                        JOIN pg_class AS pc3 ON pc.confrelid = pc3.oid
                        JOIN pg_namespace AS pn2 ON pc3.relnamespace = pn2.oid
                        CROSS JOIN LATERAL (
                            SELECT
                                json_agg(pa.attname ORDER BY c.ordinality) AS columns,
                                json_agg(pa2.attname ORDER BY c.ordinality) AS reference_columns
                            FROM
                                unnest(pc.conkey, pc.confkey) WITH ORDINALITY AS c (conkey, confkey, ordinality) 
                                JOIN pg_attribute AS pa ON c.conkey = pa.attnum
                                JOIN pg_attribute AS pa2 ON c.confkey = pa2.attnum
                            WHERE pc.conrelid = pa.attrelid AND pc.confrelid = pa2.attrelid
                        ) AS pa
                    WHERE pc.contype = 'f' AND pn.nspname NOT LIKE 'pg_%'
                ) AS pc2
        """
    )

    (schema_json,) = cur.fetchone()
    return schema_json
