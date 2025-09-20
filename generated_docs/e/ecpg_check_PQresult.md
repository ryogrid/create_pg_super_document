# ecpg_check_PQresult

## Location
[src/interfaces/ecpg/ecpglib/error.c:281-333](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/error.c#L281-L333)

## Overview
A validation function that checks PostgreSQL query results and handles various result status codes, determining whether operations succeeded or failed and triggering appropriate error handling.

## Definition

```c
bool
ecpg_check_PQresult(PGresult *results, int lineno, PGconn *connection, enum COMPAT_MODE compat)
```
## Detailed Description
The  function serves as a central validation point for all PostgreSQL query results in the ECPG library. It examines the status of PGresult objects returned by libpq functions and determines the appropriate action based on the result status. The function handles both successful operations (returning true) and various error conditions (calling appropriate error handlers and returning false). It also manages resource cleanup by calling PQclear() when necessary and provides specialized handling for different operation types such as COPY operations.

## Parameters / Member Variables
- : PGresult object containing the query result to be checked (may be NULL)
- : Line number in the source code where the check is being performed
- : PGconn object representing the database connection
- : Compatibility mode enumeration affecting error handling behavior

## Dependencies
- Functions called/Symbols referenced:
  - [ecpg_log](ecpg_log.md) (for logging error messages)
  - [ecpg_raise_backend](ecpg_raise_backend.md) (for backend-originated errors)
  - [ecpg_raise](ecpg_raise.md) (for ECPG-specific errors)
  - [PQresultStatus](../P/PQresultStatus.md) (to get result status)
  - [PQerrorMessage](../P/PQerrorMessage.md) (for connection error messages)
  - [PQresultErrorMessage](../P/PQresultErrorMessage.md) (for result-specific error messages)
  - [PQclear](../P/PQclear.md) (to free result memory)
  - PQendcopy (to end COPY operations)
- Called from (representative examples):
  - [ECPGsetcommit](../E/ECPGsetcommit.md)
  - [ECPGdescribe](../E/ECPGdescribe.md)
  - ecpg_execute
  - [ECPGtrans](../E/ECPGtrans.md)
  - prepare_common

## Notes and Other Information
- Returns true for successful operations (PGRES_TUPLES_OK, PGRES_COMMAND_OK, PGRES_COPY_OUT)
- Returns false for error conditions and cleans up resources automatically
- Handles NULL results by extracting error information from the connection object
- Special handling for PGRES_EMPTY_QUERY raises ECPG_EMPTY error
- PGRES_COPY_IN operations are terminated and treated as errors in ECPG context
- All error paths call PQclear() to prevent memory leaks
- Uses different error raising functions based on error origin (backend vs. ECPG-internal)
- Comprehensive logging for debugging and error tracking purposes