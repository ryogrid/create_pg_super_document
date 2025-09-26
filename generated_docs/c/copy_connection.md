# copy_connection

## Location
[src/test/modules/libpq_pipeline/libpq_pipeline.c:206-244](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/libpq_pipeline/libpq_pipeline.c#L206-L244)

## Overview
Creates a new PostgreSQL connection with the same connection parameters as an existing connection, used for connection duplication in test scenarios.

## Definition

```c
static PGconn *
copy_connection(PGconn *conn)
```
## Detailed Description
The  function creates a new database connection by copying all connection parameters from an existing  object. It uses the libpq  function to extract the connection information from the source connection, then reconstructs the parameters into keyword-value arrays and establishes a new connection using . This function is particularly useful in testing scenarios where multiple connections with identical parameters are needed.

The function allocates memory for keyword and value arrays, iterates through all available connection options, copies only those with non-NULL values, and terminates the arrays properly before attempting the new connection. If the new connection fails, the function terminates the program with a fatal error.

## Parameters / Member Variables
- : The source PGconn object from which to copy connection parameters

## Dependencies
- Functions called/Symbols referenced:
  - [PQconninfo](../P/PQconninfo.md)
  - [PQconninfoOption](../P/PQconninfoOption.md)
  - [pg_malloc](../p/pg_malloc.md)
  - [PQconnectdbParams](../P/PQconnectdbParams.md)
  - [PQstatus](../P/PQstatus.md)
  - CONNECTION_OK
  - [pg_fatal](../p/pg_fatal.md)
  - [PQerrorMessage](../P/PQerrorMessage.md)
- Called from (representative examples):
  - [test_cancel](../t/test_cancel.md)

## Notes and Other Information
- This is a static function within the libpq_pipeline test module
- The function performs error checking and will terminate the program if the new connection fails
- Memory is allocated for keyword and value arrays but the cleanup is not explicitly shown in this function
- Used primarily for testing pipeline functionality where multiple connections are required
- Located in src/test/modules/libpq_pipeline/libpq_pipeline.c at lines 206-244