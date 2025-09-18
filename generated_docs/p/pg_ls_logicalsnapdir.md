# pg_ls_logicalsnapdir

## Location
src/backend/utils/adt/genfile.c: 696 - 704

## Overview
A SQL-callable function that lists files in the logical replication snapshots directory.

## Definition
```c
Datum pg_ls_logicalsnapdir(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides access to the contents of PostgreSQL's logical replication snapshots directory (pg_logical/snapshots), which contains snapshot files used by logical replication. These snapshot files store the state of database transactions at specific points in time and are crucial for logical replication slots to maintain consistency when decoding WAL changes. Unlike other directory listing functions, this one sets the show_size parameter to false, indicating that file sizes are not included in the output. The snapshots directory contains files that represent consistent states of the database for logical replication purposes.

## Parameters / Member Variables
- No explicit parameters (uses PG_FUNCTION_ARGS macro for PostgreSQL function interface)

## Dependencies
- Functions called/Symbols referenced:
  - pg_ls_dir_files (performs the actual directory listing without file sizes)
- Called from (representative examples):
  - SQL queries via function call interface

## Notes and Other Information
- This function is exported (not static) and callable from SQL
- Specifically targets the pg_logical/snapshots directory for logical replication monitoring
- Uses show_size=false parameter, unlike most other directory listing functions
- Essential for monitoring logical replication health and troubleshooting replication issues
- Snapshot files in this directory are created and managed by logical replication slots
- Part of PostgreSQL's administrative function suite for logical replication management
- Useful for database administrators managing logical replication setups and monitoring replication slot states