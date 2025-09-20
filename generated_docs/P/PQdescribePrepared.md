# PQdescribePrepared

## Location
[src/interfaces/libpq/fe-exec.c:2455-2473](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L2455-L2473)

## Overview
Obtains information about a previously prepared statement by sending a Describe message to the PostgreSQL server and waiting for the response.

## Definition

```c
PGresult *
PQdescribePrepared(PGconn *conn, const char *stmt)
```
## Detailed Description
PQdescribePrepared is a synchronous function that retrieves metadata about a previously prepared statement. It sends a Describe message to the server with statement type 'S' (prepared statement) and waits for the complete response. The function returns a PGresult that contains information about the statement's input parameters and output columns.

This is a blocking operation that combines the functionality of sending the describe request and waiting for the result. If the query was not even sent due to connection issues, it returns NULL with an error message set in conn->errorMessage. On successful execution, it returns a PGresult with PGRES_COMMAND_OK status containing the statement's parameter and column metadata.

## Parameters / Member Variables
- : Connection to the PostgreSQL server
- : Name of the prepared statement to describe

## Dependencies
- Functions called/Symbols referenced:
  - [PQexecStart](PQexecStart.md)
  - [PQsendTypedCommand](PQsendTypedCommand.md)
  - PqMsg_Describe
  - [PQexecFinish](PQexecFinish.md)
- Called from (representative examples):
  - [DescribeQuery](../D/DescribeQuery.md) (src/bin/psql/common.c:1347)
  - [ECPGdescribe](../E/ECPGdescribe.md) (src/interfaces/ecpg/ecpglib/descriptor.c:912,929,957)
  - [test_prepared](../t/test_prepared.md) (src/test/modules/libpq_pipeline/libpq_pipeline.c:1335)

## Notes and Other Information
- This is a synchronous wrapper around the asynchronous PQsendDescribePrepared/PQgetResult pattern
- The caller is responsible for freeing the returned PGresult via PQclear()
- Returns NULL if the connection is not in a valid state for sending queries
- The resulting PGresult contains metadata about both input parameters and output columns of the prepared statement
- Uses the PostgreSQL protocol Describe message with type 'S' for prepared statements