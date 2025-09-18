# transfer_all_new_dbs

## Location
src/bin/pg_upgrade/relfilenumber.c: 89 - 137

## Overview
This function processes all databases in both old and new PostgreSQL clusters, generating file mappings and coordinating the transfer of individual database files during a cluster upgrade operation.

## Definition
```c
void transfer_all_new_dbs(DbInfoArr *old_db_arr, DbInfoArr *new_db_arr,
                         char *old_pgdata, char *new_pgdata, char *old_tablespace)
```

## Detailed Description
The `transfer_all_new_dbs` function iterates through all databases in the old PostgreSQL cluster and matches them with corresponding databases in the new cluster. For each matched database pair, it generates file mappings that describe how files should be transferred from the old cluster to the new cluster. The function handles cases where databases may exist in the new cluster but not in the old (such as the "postgres" database that might have been removed from the old cluster).

The function ensures proper database name matching and generates file mappings using `gen_db_file_maps` before delegating the actual file transfer to `transfer_single_new_db`. It includes error handling for cases where an old database cannot be found in the new cluster.

## Parameters / Member Variables
- `old_db_arr`: Array containing information about databases in the old PostgreSQL cluster
- `new_db_arr`: Array containing information about databases in the new PostgreSQL cluster
- `old_pgdata`: Path to the old PostgreSQL data directory
- `new_pgdata`: Path to the new PostgreSQL data directory
- `old_tablespace`: Path to the old tablespace being processed (can be NULL for all tablespaces)

## Dependencies
- Functions called/Symbols referenced:
  - [gen_db_file_maps](../g/gen_db_file_maps.md)
  - [transfer_single_new_db](transfer_single_new_db.md)
  - [pg_free](../p/pg_free.md)
  - [DbInfo](../D/DbInfo.md), FileNameMap, DbInfoArr
- Called from (representative examples):
  - [parallel_transfer_all_new_dbs](../p/parallel_transfer_all_new_dbs.md)
  - [win32_transfer_all_new_dbs](../w/win32_transfer_all_new_dbs.md)

## Notes and Other Information
- The function handles database name mismatches gracefully by advancing through the new database array to find matching names
- It generates file mappings for each database pair and only calls transfer_single_new_db if there are mappings to process
- Memory allocated for mappings is properly freed after use, even when n_maps is 0
- The function will terminate with pg_fatal if an old database cannot be found in the new cluster
- The old_tablespace parameter allows filtering transfers to a specific tablespace, supporting parallel processing