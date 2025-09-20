# old_9_6_invalidate_hash_indexes

## Location
[src/bin/pg_upgrade/version.c:37-146](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/version.c#L37-L146)

## Overview
A PostgreSQL upgrade utility function that handles hash index incompatibility between PostgreSQL 9.6 and 10.0 by invalidating old hash indexes and generating reindex scripts.

## Definition

```c
structions.");
```
## Detailed Description
This function addresses a critical compatibility issue during PostgreSQL upgrades from version 9.6 to 10.0, where the internal binary format of hash indexes changed significantly. The function performs a two-phase operation: first, it scans all databases in the cluster to identify hash indexes, and second, it either reports the issue (in check mode) or takes corrective action by invalidating the indexes and generating a reindex script.

When not in check mode, the function creates a "reindex_hash.sql" script containing REINDEX commands for all affected hash indexes. It also marks these indexes as invalid in the system catalogs to prevent their use until they are properly reindexed with the new format.

## Parameters / Member Variables
- : Pointer to ClusterInfo structure containing information about the PostgreSQL cluster being upgraded
- : Boolean flag indicating whether to perform actual modifications (false) or just report findings (true)

## Dependencies
- Functions called/Symbols referenced:
  - [prep_status](../p/prep_status.md) (status reporting)
  - [connectToServer](../c/connectToServer.md) (database connection)
  - [executeQueryOrDie](../e/executeQueryOrDie.md) (SQL query execution)
  - fopen_priv (secure file opening)
  - [PQExpBufferData](../P/PQExpBufferData.md), initPQExpBuffer, appendPsqlMetaConnect, termPQExpBuffer (query buffer management)
  - [quote_identifier](../q/quote_identifier.md) (SQL identifier quoting)
  - [PQfinish](../P/PQfinish.md) (connection cleanup)
  - report_status, pg_log (logging functions)
  - [check_ok](../c/check_ok.md) (status completion)
- Called from (representative examples):
  - [check_and_dump_old_cluster](../c/check_and_dump_old_cluster.md) (during upgrade checks)
  - [issue_warnings_and_set_wal_level](../i/issue_warnings_and_set_wal_level.md) (during upgrade process)

## Notes and Other Information
- Specifically targets the hash index format change between PostgreSQL 9.6 and 10.0
- Uses SQL queries to identify hash indexes by joining pg_class, pg_index, pg_am, and pg_namespace catalogs
- In non-check mode, generates both a reindex script and marks indexes as invalid to ensure data consistency
- Provides detailed user warnings about the need to reindex hash indexes after upgrade
- Part of the pg_upgrade utility's version-specific compatibility handling framework
- The generated reindex script must be executed by a database superuser after the upgrade completes