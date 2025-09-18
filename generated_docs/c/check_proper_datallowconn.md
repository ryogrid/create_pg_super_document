# check_proper_datallowconn

## Location
[src/bin/pg_upgrade/check.c:1095-1178](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/check.c#L1095-L1178)

## Overview
This function validates database connection settings to ensure that all non-template0 databases allow connections and that template0 explicitly disallows connections, preventing pg_dumpall restore failures.

## Definition
```c
static void check_proper_datallowconn(ClusterInfo *cluster)
```

## Detailed Description
The `check_proper_datallowconn` function performs essential validation of the `datallowconn` setting across all databases in a PostgreSQL cluster to ensure successful cluster upgrade operations. This function addresses two critical requirements:

1. **template0 Connection Restriction**: Ensures template0 database has `datallowconn=false`. If template0 allows connections, pg_dumpall will fail when attempting to restore globals because it tries to recreate template0 but cannot drop it while connections exist.

2. **Non-template0 Connection Requirement**: Ensures all other databases have `datallowconn=true`. Databases with `datallowconn=false` will be skipped during the restore process, leading to data loss.

The function queries `pg_database` to examine all databases and their connection settings. When problematic databases are found (non-template0 databases with `datallowconn=false`), it creates a report file listing these databases and terminates the upgrade with detailed instructions for resolution.

## Parameters / Member Variables
- `cluster`: Pointer to ClusterInfo structure containing cluster connection and configuration information

## Dependencies
- Functions called/Symbols referenced:
  - [prep_status](../p/prep_status.md) (status reporting for user feedback)  
  - [connectToServer](connectToServer.md) (establishes database connection)
  - [executeQueryOrDie](../e/executeQueryOrDie.md) (SQL query execution with error handling)
  - [PQfnumber](../P/PQfnumber.md) (column index lookup)
  - [PQntuples](../P/PQntuples.md) (result tuple count)
  - [PQgetvalue](../P/PQgetvalue.md) (result value extraction)
  - [PQclear](../P/PQclear.md) (result cleanup)
  - [PQfinish](../P/PQfinish.md) (connection cleanup)
  - fopen_priv (secure file opening)
  - fclose (file closing)
  - [pg_log](../p/pg_log.md) (logging with severity levels)
  - [pg_fatal](../p/pg_fatal.md) (error reporting and termination)
  - [check_ok](check_ok.md) (completion status reporting)
- Called from (representative examples):
  - [check_and_dump_old_cluster](check_and_dump_old_cluster.md) (old cluster validation)

## Notes and Other Information
- This is a static function, only accessible within the check.c compilation unit
- Creates a report file "databases_with_datallowconn_false.txt" in the log directory when issues are found
- Connects specifically to template1 database for querying pg_database
- The validation prevents silent data loss during cluster upgrade operations
- Template0 is treated as a special case due to pg_dumpall restore requirements
- Function provides detailed user guidance for resolving datallowconn issues
- Uses MAXPGPATH constant for output path buffer sizing