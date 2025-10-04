# PQclosePrepared

## Location
[src/interfaces/libpq/fe-exec.c:2521-2538](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L2521-L2538)

## Overview
Closes a previously prepared statement by sending a Close message to the PostgreSQL server and waiting for the response.

## Definition
PGresult *PQclosePrepared(PGconn *conn, const char *stmt)

## Detailed Description
PQclosePrepared is a synchronous function that closes a previously prepared statement on the PostgreSQL server. It sends a Close message to the server with statement type 'S' (prepared statement) and waits for the complete response. This function is used to explicitly release server resources associated with a prepared statement when it is no longer needed.

The function returns a PGresult that indicates whether the close operation was successful. If the query was not even sent due to connection issues, it returns NULL with an error message set in conn->errorMessage. On successful execution, it returns a PGresult with PGRES_COMMAND_OK status.

## Parameters / Member Variables
- `conn`: Connection to the PostgreSQL server
- `stmt`: Name of the prepared statement to close

## Dependencies
- Functions called/Symbols referenced:
  - [PQexecStart](PQexecStart.md)
  - [PQsendTypedCommand](PQsendTypedCommand.md)
  - PqMsg_Close
  - [PQexecFinish](PQexecFinish.md)
- Called from (representative examples):
  - [test_prepared](../t/test_prepared.md) (src/test/modules/libpq_pipeline/libpq_pipeline.c:1343)

## Notes and Other Information
- This is a synchronous operation that blocks until the server responds
- The caller is responsible for freeing the returned PGresult via PQclear()
- Returns NULL if the connection is not in a valid state for sending queries
- Uses the PostgreSQL protocol Close message with type 'S' for prepared statements
- Explicitly releases server resources associated with the prepared statement
- Should be called when a prepared statement is no longer needed to free server resources
- The server automatically closes prepared statements when the connection is terminated

## Simplified Source

```c
PGresult *
PQclosePrepared(PGconn *conn, const char *stmt)
{
    // Prepare connection for query execution
    if (!PQexecStart(conn))
        return NULL;

    // Send Close message for prepared statement ('S' type)
    if (!PQsendTypedCommand(conn, PqMsg_Close, 'S', stmt))
        return NULL;

    // Wait for and retrieve the close result
    return PQexecFinish(conn);
}
```