# ecpg_check_PQresult

## Location
src/interfaces/ecpg/ecpglib/error.c: 281 - 333

## Overview
A validation function that checks PostgreSQL query results and handles various result status codes, determining whether operations succeeded or failed and triggering appropriate error handling.

## Definition


## Detailed Description
The  function serves as a central validation point for all PostgreSQL query results in the ECPG library. It examines the status of PGresult objects returned by libpq functions and determines the appropriate action based on the result status. The function handles both successful operations (returning true) and various error conditions (calling appropriate error handlers and returning false). It also manages resource cleanup by calling PQclear() when necessary and provides specialized handling for different operation types such as COPY operations.

## Parameters / Member Variables
- : PGresult object containing the query result to be checked (may be NULL)
- : Line number in the source code where the check is being performed
- : PGconn object representing the database connection
- : Compatibility mode enumeration affecting error handling behavior

## Dependencies
- Functions called/Symbols referenced:
  - ecpg_log (for logging error messages)
  - ecpg_raise_backend (for backend-originated errors)
  - ecpg_raise (for ECPG-specific errors)
  - PQresultStatus (to get result status)
  - PQerrorMessage (for connection error messages)
  - PQresultErrorMessage (for result-specific error messages)
  - PQclear (to free result memory)
  - PQendcopy (to end COPY operations)
- Called from (representative examples):
  - ECPGsetcommit
  - ECPGdescribe
  - ecpg_execute
  - ECPGtrans
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