# get_primary_sysid

## Location
[src/bin/pg_basebackup/pg_createsubscriber.c:560-600](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_createsubscriber.c#L560-L600)

## Overview
Retrieves the system identifier from a PostgreSQL publisher database, which is used to verify that a data directory is a clone of another database instance.

## Definition

```c
static uint64
get_primary_sysid(const char *conninfo)
```
## Detailed Description
The  function connects to a PostgreSQL publisher database and retrieves its system identifier by querying the  function. The system identifier is a unique 64-bit integer that PostgreSQL assigns to each database cluster during initialization (). This identifier is crucial for pg_createsubscriber to verify that the subscriber's data directory is indeed a physical copy (base backup) of the publisher database.

The function includes comprehensive error handling for connection failures, query execution problems, and unexpected result sets. It uses logging to provide visibility into the operation and automatically terminates the program on critical errors using the  utility function.

## Parameters / Member Variables
- `*conninfo`: A PostgreSQL connection string specifying how to connect to the publisher database (includes host, port, database name, credentials, etc.)
## Dependencies
- Functions called/Symbols referenced:
  - pg_log_info (logging function for informational messages)
  - [connect_database](../c/connect_database.md) (utility function to establish database connection)
  - [PQexec](../P/PQexec.md) (libpq function for executing SQL queries)
  - PGRES_TUPLES_OK (libpq constant for successful SELECT result)
  - [PQresultErrorMessage](../P/PQresultErrorMessage.md) (libpq function to get error message from result)
  - [PQntuples](../P/PQntuples.md) (libpq function to get number of rows in result)
  - [PQgetvalue](../P/PQgetvalue.md) (libpq function to get field value from result)
  - strtou64 (PostgreSQL utility function to convert string to uint64)
  - [PQclear](../P/PQclear.md) (libpq function to free result memory)
  - [disconnect_database](../d/disconnect_database.md) (utility function for connection cleanup)

- Called from (representative examples):
  - [main](../m/main.md) (primary entry point of pg_createsubscriber)
  - [LogicalRepInfo](../L/LogicalRepInfo.md) structure initialization

## Notes and Other Information
- This is a static function, only accessible within pg_createsubscriber.c
- The function queries , which is a PostgreSQL system function that returns control file information including the system identifier
- System identifiers are essential for logical replication setup to ensure data consistency and prevent replication between unrelated database clusters
- The function expects exactly one row in the result set - any other count indicates an error condition
- Error conditions result in program termination via 
- Located in src/bin/pg_basebackup/pg_createsubscriber.c:560-600
- The returned system identifier is logged for diagnostic purposes