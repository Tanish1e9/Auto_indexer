CREATE FUNCTION auto_index_cleanup() RETURNS event_trigger
    AS 'MODULE_PATHNAME', 'auto_index_cleanup'
    LANGUAGE C STRICT;

CREATE EVENT TRIGGER auto_index_cleanup_tri ON sql_drop
    WHEN TAG IN ('DROP EXTENSION')
    EXECUTE FUNCTION auto_index_cleanup();

ALTER EXTENSION auto_index DROP EVENT TRIGGER auto_index_cleanup_tri;
ALTER EXTENSION auto_index DROP FUNCTION auto_index_cleanup();

CREATE TABLE IF NOT EXISTS aidx_queries (
    tablename TEXT,
    colname TEXT,
    cost FLOAT,
    benefit FLOAT,
    num_queries INT,
    is_indexed BOOLEAN,
    PRIMARY KEY (tablename, colname)
);
