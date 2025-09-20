# enable_subscription

## Location
[src/bin/pg_basebackup/pg_createsubscriber.c:1840-1874](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_createsubscriber.c#L1840-L1874)

## Overview
The enable_subscription function activates a previously created but disabled subscription in PostgreSQL logical replication.

## Definition

```c
static void
enable_subscription(PGconn *conn, const struct LogicalRepInfo *dbinfo)
```
## Detailed Description
This function is part of the pg_createsubscriber utility and is responsible for enabling a logical replication subscription that was created in a disabled state during an earlier step of the subscription setup process. The function executes an ALTER SUBSCRIPTION ENABLE command to activate the subscription after the initial logical replication location has been properly adjusted. It includes comprehensive error handling and logging to track the operation's progress and handle potential failures during the enable operation.

## Parameters / Member Variables
- : PGconn pointer representing the database connection to execute the enable command
- : Pointer to LogicalRepInfo struct containing subscription details including subscription name and database name

## Dependencies
- Functions called/Symbols referenced:
  - [PQescapeIdentifier](../P/PQescapeIdentifier.md) (escapes subscription name for SQL safety)
  - pg_log_info (logs informational messages)
  - pg_log_debug (logs debug-level command information)
  - [PQexec](../P/PQexec.md) (executes the ALTER SUBSCRIPTION command)
  - [PQresultStatus](../P/PQresultStatus.md) (checks command execution result)
  - [PQresultErrorMessage](../P/PQresultErrorMessage.md) (retrieves error messages on failure)
  - [disconnect_database](../d/disconnect_database.md) (handles database disconnection on errors)
  - [PQfreemem](../P/PQfreemem.md) (frees escaped identifier memory)
  - createPQExpBuffer/destroyPQExpBuffer (manages query buffer)
- Called from (representative examples):
  - [setup_subscriber](../s/setup_subscriber.md) (main subscription setup workflow)

## Notes and Other Information
- The function is marked as static, indicating it's only used within the pg_createsubscriber.c file
- Includes dry-run support through the global dry_run variable - when enabled, the command is logged but not executed
- Uses proper SQL identifier escaping to prevent SQL injection attacks
- Implements robust error handling that terminates the database connection on command failure
- Part of the larger pg_createsubscriber utility workflow for converting a physical replica to a logical subscriber