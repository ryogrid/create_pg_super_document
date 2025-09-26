# setup_connection

## Location
[src/bin/pg_dump/pg_dump.c:1212-1381](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L1212-L1381)

## Overview
Establishes and configures a database connection for pg_dump operations, setting up appropriate encoding, transaction isolation, timeouts, and other parameters needed for consistent data dumping.

## Definition

```c
static void
setup_connection(Archive *AH, const char *dumpencoding,
				 const char *dumpsnapshot, char *use_role)
```
## Detailed Description
The setup_connection function performs comprehensive initialization of a database connection specifically for pg_dump operations. It configures various PostgreSQL settings to ensure consistent, portable, and secure data dumping. The function sets client encoding, establishes proper transaction isolation levels, configures timeouts, and handles snapshot synchronization for parallel dumps. It also applies security measures by restricting access to certain relation types and setting up role-based access if specified.

The function handles both single-process and parallel dump scenarios, with special logic for coordinating snapshots across multiple worker processes. It sets various PostgreSQL parameters to ensure the dump output is deterministic and portable across different PostgreSQL installations.

## Parameters / Member Variables
- : Archive handle containing connection information and dump configuration
- : Client encoding to use for the connection (can be NULL)
- : Specific snapshot ID to use for consistent reads (can be NULL)
- : Database role to assume for the dump operation (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [GetConnection](../G/GetConnection.md) (retrieve database connection from archive)
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md) (execute SQL returning single row)
  - [ExecuteSqlStatement](../E/ExecuteSqlStatement.md) (execute SQL statement without return)
  - [PQsetClientEncoding](../P/PQsetClientEncoding.md) (set client character encoding)
  - [PQclientEncoding](../P/PQclientEncoding.md) (get current client encoding)
  - [setFmtEncoding](setFmtEncoding.md) (set encoding for formatting functions)
  - [PQparameterStatus](../P/PQparameterStatus.md) (get server parameter value)
  - [fmtId](../f/fmtId.md) (format identifier with proper quoting)
  - [set_restrict_relation_kind](set_restrict_relation_kind.md) (restrict access to relation types)
  - [get_synchronized_snapshot](../g/get_synchronized_snapshot.md) (obtain synchronized snapshot for parallel dumps)
  - [pg_malloc0](../p/pg_malloc0.md) (allocate zero-initialized memory)
  - [pg_strdup](../p/pg_strdup.md) (duplicate string)
- Called from (representative examples):
  - [main](../m/main.md) (primary setup for main dump process)
  - [setupDumpWorker](setupDumpWorker.md) (setup for parallel dump worker processes)

## Notes and Other Information
- Function is marked static, limiting scope to pg_dump.c file
- Sets search_path to secure default to prevent malicious function calls
- Configures DATESTYLE to ISO and INTERVALSTYLE to POSTGRES for portability
- Sets extra_float_digits for exact floating-point representation
- Disables synchronized sequential scans for consistent ordering
- Sets various timeout parameters to 0 to prevent interruption
- Handles version-specific features based on remote server version
- Supports row-level security configuration for PostgreSQL 9.5+
- Initializes prepared query tracking state for the connection
- Establishes transaction with appropriate isolation level (SERIALIZABLE or REPEATABLE READ)
- Handles snapshot coordination for parallel dumps through snapshot IDs
- Includes special handling for standby servers in parallel dump scenarios