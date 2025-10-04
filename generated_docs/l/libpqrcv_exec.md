# libpqrcv_exec

## Location
[src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:1235-1314](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/libpqwalreceiver/libpqwalreceiver.c#L1235-L1314)

## Overview
A public interface function for sending generic SQL queries and commands to a database connection through the PostgreSQL WAL receiver library.

## Definition

```c
static WalRcvExecResult *
libpqrcv_exec(WalReceiverConn *conn, const char *query,
			  const int nRetTypes, const Oid *retTypes)
```
## Detailed Description
The  function provides a unified interface for executing SQL queries and commands through a WAL receiver connection. It validates that the calling process is connected to a database, executes the query using the underlying libpq connection, and processes the results into a standardized  structure. The function handles various PostgreSQL result types including tuples, copy operations, and command results, converting them into appropriate WAL receiver status codes. It also captures and processes error information including SQL state codes for failed queries.

## Parameters / Member Variables
- `*conn`: A pointer to the WalReceiverConn structure containing the active database connection
- `*query`: A null-terminated string containing the SQL query or command to execute
- `nRetTypes`: The number of expected return types for tuple results
- `*retTypes`: An array of PostgreSQL type OIDs specifying the expected column types
## Dependencies
- Functions called/Symbols referenced:
  - [libpqrcv_PQexec](libpqrcv_PQexec.md)
  - [libpqrcv_processTuples](libpqrcv_processTuples.md)
  - [PQresultStatus](../P/PQresultStatus.md)
  - [PQerrorMessage](../P/PQerrorMessage.md)
  - [PQresultErrorField](../P/PQresultErrorField.md)
  - [PQclear](../P/PQclear.md)
  - [pchomp](../p/pchomp.md)
  - MAKE_SQLSTATE
  - [palloc0](../p/palloc0.md)
  - ereport
- Called from (representative examples):
  - [WalReceiverConn](../W/WalReceiverConn.md) functions (line 85, 107)

## Notes and Other Information
- This function can only be called from a process connected to a database (MyDatabaseId != InvalidOid)
- Empty queries are treated as errors rather than valid operations
- The function handles all major PostgreSQL result status types including pipeline modes (which are treated as errors)
- Error handling includes capturing SQL state codes for diagnostic purposes
- Memory allocation for the result structure uses palloc0 for zero-initialization
- The function is static, indicating it's only used within the libpqwalreceiver module

## Simplified Source
```c
static WalRcvExecResult *
libpqrcv_exec(WalReceiverConn *conn, const char *query,
              const int nRetTypes, const Oid *retTypes)
{
    WalRcvExecResult *walres = palloc0(sizeof(WalRcvExecResult));

    /* Ensure we have a database connection */
    if (MyDatabaseId == InvalidOid)
        ereport(ERROR, "the query interface requires a database connection");

    /* Execute the query */
    PGresult *pgres = libpqrcv_PQexec(conn->streamConn, query);

    /* Process results based on status */
    switch (PQresultStatus(pgres))
    {
        case PGRES_TUPLES_OK:
        case PGRES_SINGLE_TUPLE:
        case PGRES_TUPLES_CHUNK:
            walres->status = WALRCV_OK_TUPLES;
            libpqrcv_processTuples(pgres, walres, nRetTypes, retTypes);
            break;

        case PGRES_COPY_IN:
            walres->status = WALRCV_OK_COPY_IN;
            break;

        case PGRES_COPY_OUT:
            walres->status = WALRCV_OK_COPY_OUT;
            break;

        case PGRES_COPY_BOTH:
            walres->status = WALRCV_OK_COPY_BOTH;
            break;

        case PGRES_COMMAND_OK:
            walres->status = WALRCV_OK_COMMAND;
            break;

        case PGRES_EMPTY_QUERY:
            walres->status = WALRCV_ERROR;
            walres->err = "empty query";
            break;

        default:
            /* Handle all error cases */
            walres->status = WALRCV_ERROR;
            walres->err = pchomp(PQerrorMessage(conn->streamConn));

            /* Extract SQL state if available */
            char *diag_sqlstate = PQresultErrorField(pgres, PG_DIAG_SQLSTATE);
            if (diag_sqlstate)
                walres->sqlstate = MAKE_SQLSTATE(diag_sqlstate[0], diag_sqlstate[1],
                                               diag_sqlstate[2], diag_sqlstate[3],
                                               diag_sqlstate[4]);
            break;
    }

    PQclear(pgres);
    return walres;
}
```