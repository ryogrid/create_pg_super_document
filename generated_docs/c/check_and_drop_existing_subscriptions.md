# check_and_drop_existing_subscriptions

## Location
[src/bin/pg_basebackup/pg_createsubscriber.c:1103-1142](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_createsubscriber.c#L1103-L1142)

## Overview
Retrieves and drops all pre-existing subscriptions for a specified database in PostgreSQL's logical replication setup.

## Definition


## Detailed Description
This function is part of the pg_createsubscriber utility that converts a standby server into a logical replica. It performs cleanup by identifying and removing any existing subscriptions in the target database. The function queries the pg_subscription catalog to find subscriptions associated with the specified database, then calls drop_existing_subscriptions() to remove each one. This ensures a clean state before setting up new logical replication subscriptions.

## Parameters / Member Variables
- : PostgreSQL database connection handle used to execute queries
- : Pointer to LogicalRepInfo structure containing database information, specifically the database name to check for subscriptions

## Dependencies
- Functions called/Symbols referenced:
  - [PQescapeLiteral](../P/PQescapeLiteral.md) (escapes database name for safe SQL usage)
  - [PQexec](../P/PQexec.md) (executes the subscription query)
  - [PQresultStatus](../P/PQresultStatus.md)/PGRES_TUPLES_OK (checks query result status)
  - [PQresultErrorMessage](../P/PQresultErrorMessage.md) (retrieves error messages on failure)
  - [disconnect_database](../d/disconnect_database.md) (handles connection cleanup on error)
  - [drop_existing_subscriptions](../d/drop_existing_subscriptions.md) (removes individual subscriptions)
  - [PQfreemem](../P/PQfreemem.md) (frees escaped string memory)
- Called from:
  - [setup_subscriber](../s/setup_subscriber.md) (main subscription setup function)

## Notes and Other Information
- This is a static function, only accessible within pg_createsubscriber.c
- Performs proper error handling and resource cleanup
- Uses parameterized queries with escaped literals for SQL injection prevention
- Part of the logical replication infrastructure for converting standby to subscriber