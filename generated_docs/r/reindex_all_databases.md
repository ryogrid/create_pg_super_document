# reindex_all_databases

## Location
[src/bin/scripts/reindexdb.c:820-883](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/scripts/reindexdb.c#L820-L883)

## Overview
Reindexes all databases in a PostgreSQL cluster that allow connections, iterating through each database and applying the specified reindexing operations.

## Definition


## Detailed Description
This function is part of the  command-line utility and handles the "--all" option to reindex all databases in a PostgreSQL cluster. It first connects to a maintenance database to query the system catalog for a list of all databases that allow connections (excluding template databases with datconnlimit = -2). For each database found, it performs the requested reindexing operations in a specific order: system catalogs first (if requested), then schemas, indexes, tables, and finally the entire database (if no specific objects were specified).

The function respects the hierarchy of reindexing operations and ensures that more specific operations (indexes, tables, schemas) take precedence over general database-wide reindexing. It also handles concurrent reindexing options and can optionally move indexes to a different tablespace during the reindex operation.

## Parameters / Member Variables
- : Connection parameters structure containing database connection information
- : Name of the program (typically "reindexdb") for error messages and output
- : If true, echo the SQL commands being executed to stdout
- : If true, suppress informational output messages
- : If true, provide detailed progress information
- : If true, perform reindexing concurrently (non-blocking)
- : Number of concurrent connections to use for parallel operations
- : Optional tablespace name to move indexes to during reindexing
- : If true, reindex system catalogs
- : List of specific schemas to reindex
- : List of specific tables to reindex  
- : List of specific indexes to reindex

## Dependencies
- Functions called/Symbols referenced:
  - [connectMaintenanceDatabase](../c/connectMaintenanceDatabase.md)
  - [executeQuery](../e/executeQuery.md)
  - [PQfinish](../P/PQfinish.md)
  - [reindex_one_database](reindex_one_database.md)
  - REINDEX_SYSTEM
  - REINDEX_SCHEMA
  - REINDEX_INDEX
  - REINDEX_TABLE
  - REINDEX_DATABASE
- Called from (representative examples):
  - [main](../m/main.md) (in reindexdb.c)

## Notes and Other Information
- This is a static function internal to the reindexdb utility
- The function queries pg_database to find all databases where datallowconn=true and datconnlimit≠-2
- Reindexing operations are performed in a specific priority order: system catalogs, schemas, indexes, tables, then full database
- Full database reindexing only occurs if no specific objects (schemas, tables, indexes) are specified and syscatalog is false
- The function temporarily overrides the database name in cparams for each database being processed
- Uses different concurrency settings for different operations (system catalogs and indexes use concurrency=1)