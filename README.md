# Auto_Index PostgreSQL Extension

## Overview

`auto_index` is a PostgreSQL extension that automatically identifies beneficial indexes during query execution and creates them dynamically using background workers.

## Files

| File                     | Description                                                                 |
|--------------------------|-----------------------------------------------------------------------------|
| `auto_index.c`           | Core implementation of planner/executor hooks, background worker logic, and index analysis. |
| `auto_index.h`           | Header file declaring functions, hooks, and required PostgreSQL includes.   |
| `auto_index.control`     | Extension control file with metadata for PostgreSQL.                        |
| `auto_index--1.0.sql`    | SQL script to set up the extension and required database objects.           |
| `Makefile`               | Build configuration for compiling the extension with PGXS.                  |
| `run_query.py`           | Spawns threads and runs queries from each of the threads. Helpful for running queries large number of times.                  |

## Running Instructions

The following steps describe how to integrate the `auto_index` extension into your PostgreSQL installation.

1. **Adding the Extension**
   - Copy the `auto_index` folder into the `contrib` directory of your PostgreSQL source tree.
   - Add `auto_index` to the `shared_preload_libraries` setting in the `postgresql.conf` file of your database:
     ```conf
     shared_preload_libraries = 'auto_index'
     ```

2. **Compilation and Installation**
   - Open a terminal and navigate to the `auto_index` directory.
   - Run the following commands:
     ```bash
     make
     sudo make install
     ```

3. **Initialize the Extension**
   - To initialize the extension (e.g., every time you start using it), connect to your database and run:
     ```sql
     CREATE EXTENSION auto_index;
     ```
   - This sets up the required `aidx_queries` table used to track indexing opportunities.

4. **Cleanup**
   - To remove the extension and clean up allocated resources, run:
     ```sql
     DROP EXTENSION auto_index;
     ```
   - This will remove the `aidx_queries` table and deactivate the extension.
