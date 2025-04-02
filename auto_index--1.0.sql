CREATE TABLE aidx_queries (
    tablename TEXT,
    colname TEXT,
    cost FLOAT,
    benefit FLOAT,
    num_queries INT,
    PRIMARY KEY (tablename, colname)
);
