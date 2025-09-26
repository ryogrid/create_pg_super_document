# PQexecParams

## Location
[src/interfaces/libpq/fe-exec.c:2276-2305](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L2276-L2305)

## Overview
Executes a SQL command with parameters using PostgreSQL's extended query protocol, providing a safer alternative to PQexec for parameterized queries.

## Definition

```c
PGresult *
PQexecParams(PGconn *conn,
			 const char *command,
			 int nParams,
			 const Oid *paramTypes,
			 const char *const *paramValues,
			 const int *paramLengths,
			 const int *paramFormats,
			 int resultFormat)
```
## Detailed Description
PQexecParams provides a way to execute SQL commands with parameters using PostgreSQL's extended query protocol. Unlike PQexec, which requires manual escaping of parameter values, PQexecParams automatically handles parameter serialization and prevents SQL injection attacks. The function follows a synchronous execution model - it sends the query and waits for the complete result before returning.

The function internally uses PQexecStart to prepare the connection, PQsendQueryParams to send the parameterized query, and PQexecFinish to wait for and retrieve the result.

## Parameters / Member Variables
- : PostgreSQL connection object that must be in a valid state
- : SQL command string that may contain parameter placeholders (, , etc.)
- : Number of parameters in the query (must match placeholder count)
- : Array of parameter type OIDs, or NULL to let server infer types
- : Array of parameter value strings, with NULL indicating SQL NULL
- : Array of parameter lengths for binary format, ignored for text format
- : Array indicating text (0) or binary (1) format for each parameter
- : Format for result columns: 0 for text, 1 for binary

## Dependencies
- Functions called/Symbols referenced:
  - [PQexecStart](PQexecStart.md)
  - [PQsendQueryParams](PQsendQueryParams.md)  
  - [PQexecFinish](PQexecFinish.md)
- Called from (representative examples):
  - [libpq_fetch_file](../l/libpq_fetch_file.md)
  - [ecpg_execute](../e/ecpg_execute.md)
  - [main](../m/main.md) (testlibpq3.c)
  - [main](../m/main.md) (isolationtester.c)

## Notes and Other Information
- Returns NULL if the query cannot be sent or if connection preparation fails
- The returned PGresult must be freed using PQclear() when no longer needed
- Parameter placeholders in the command string use PostgreSQL's  syntax (, , etc.)
- Using paramTypes as NULL allows PostgreSQL to infer parameter types automatically
- Binary parameter formats require careful handling of endianness and type-specific encoding
- This function blocks until the query completes, making it unsuitable for non-blocking applications