# 1c-to-postgres-etl
ETL pipeline from 1C to PostgreSQL via API

CREATE OR REPLACE PROCEDURE capture_index_history()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO index_history (
        table_schema,
        table_name,
        index_name,
        index_definition,
        indexed_columns
    )
    SELECT
        n.nspname AS table_schema,
        c.relname AS table_name,
        i.relname AS index_name,
        pg_get_indexdef(i.oid) AS index_definition,
        array_agg(a.attname ORDER BY p.pnum) AS indexed_columns
    FROM
        pg_index x
    JOIN pg_class c ON c.oid = x.indrelid
    JOIN pg_class i ON i.oid = x.indexrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN LATERAL unnest(x.indkey) WITH ORDINALITY AS p(attnum, pnum) ON true
    JOIN pg_attribute a ON a.attrelid = x.indrelid AND a.attnum = p.attnum
    WHERE
        c.relkind = 'r'::"char"
        AND i.relkind = 'i'::"char"
        AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    GROUP BY
        n.nspname,
        c.relname,
        i.relname,
        x.oid;
END;
$$;

