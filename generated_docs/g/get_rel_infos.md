# get_rel_infos

## Location
[src/bin/pg_upgrade/info.c:445-639](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/info.c#L445-L639)

## Overview
The get_rel_infos function collects metadata for all user tables, materialized views, toast tables, and indexes within a specific database during the PostgreSQL upgrade process.

## Definition

```c
enumber,
				i_reltablespace;
```
## Detailed Description
This function is a crucial component of pg_upgrade that gathers comprehensive relation metadata from a database. It constructs and executes a complex SQL query using Common Table Expressions (CTEs) to collect information about regular heap tables, toast tables, and indexes. The function categorizes relations into three groups: regular_heap (user tables and materialized views), toast_heap (toast tables for large objects), and all_index (valid indexes). It optimizes memory usage by reusing string allocations for identical namespace and tablespace names. The results are guaranteed to be sorted by OID to enable efficient matching between old and new databases during the upgrade process.

## Parameters / Member Variables
- : Pointer to ClusterInfo structure containing cluster connection and version information
- : Pointer to DbInfo structure representing the specific database being processed

## Dependencies
- Functions called/Symbols referenced:
  - [connectToServer](../c/connectToServer.md)
  - [executeQueryOrDie](../e/executeQueryOrDie.md)
  - pg_malloc
  - [pg_strdup](../p/pg_strdup.md)
  - atooid
  - [PQfnumber](../P/PQfnumber.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [PQclear](../P/PQclear.md)
  - [PQfinish](../P/PQfinish.md)
  - CppAsString2
  - strcmp
- Called from (representative examples):
  - [get_db_rel_and_slot_infos](get_db_rel_and_slot_infos.md)

## Notes and Other Information
- Results are sorted by OID to enable efficient old/new database matching
- Uses memory optimization by reusing identical namespace and tablespace string pointers
- Filters relations based on FirstNormalObjectId to exclude system objects
- Handles pg_largeobject specially as it contains user data not in pg_dump output
- Excludes temporary tables, invalid indexes, and system schemas (pg_catalog, information_schema, etc.)
- Only processes valid and ready indexes (indisvalid AND indisready)
- Uses complex CTE-based SQL query to handle different relation types efficiently
- Function is static, indicating internal use within the info.c compilation unit