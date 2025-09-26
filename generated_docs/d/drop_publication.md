# drop_publication

## Location
[src/bin/pg_basebackup/pg_createsubscriber.c:1637-1690](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_createsubscriber.c#L1637-L1690)

## Overview
drop_publication is a cleanup function that removes a PostgreSQL publication when the pg_createsubscriber process cannot complete all required steps successfully.

## Definition
```c
static void drop_publication(PGconn *conn, struct LogicalRepInfo *dbinfo)
```

## Detailed Description
This function is designed to remove publications created during the pg_createsubscriber process when cleanup is necessary due to errors or failures. Unlike typical error handling that would terminate immediately upon failure, this function is intentionally designed to be resilient and continue execution even if the DROP PUBLICATION command fails. This behavior allows the process to continue and provide useful instructions to users for manual cleanup if needed.

The function constructs a DROP PUBLICATION SQL statement with proper identifier escaping and executes it against the specified database connection. It respects dry run mode by skipping the actual execution while still performing all logging and validation steps. If the drop operation fails, it updates the LogicalRepInfo structure to prevent retry attempts but does not terminate the program.

## Parameters / Member Variables
- `conn`: Active PostgreSQL database connection used to execute the DROP PUBLICATION command
- `dbinfo`: Pointer to LogicalRepInfo structure containing publication information including name and database name

## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [PQescapeIdentifier](../P/PQescapeIdentifier.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - pg_log_info
  - pg_log_debug
  - pg_log_error
  - [PQexec](../P/PQexec.md)
  - [PQresultStatus](../P/PQresultStatus.md)
  - [PQresultErrorMessage](../P/PQresultErrorMessage.md)
  - [PQclear](../P/PQclear.md)
  - [PQfreemem](../P/PQfreemem.md)
  - [destroyPQExpBuffer](destroyPQExpBuffer.md)
- Called from (representative examples):
  - [cleanup_objects_atexit](../c/cleanup_objects_atexit.md)
  - [setup_subscriber](../s/setup_subscriber.md)

## Notes and Other Information
- Used for both primary server cleanup (publication/replication slot errors) and subscriber cleanup (replicated publications removal)
- Designed to be non-fatal: continues execution even if DROP PUBLICATION fails
- Sets made_publication flag to false on failure to prevent retry attempts
- Supports dry run mode for testing without making actual changes
- Provides detailed logging for both success and failure scenarios
- [Publication](../P/Publication.md) names are properly escaped to prevent SQL injection
- Part of the cleanup infrastructure for pg_createsubscriber error recovery