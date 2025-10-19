# ClearOrSaveResult

## Location
[src/bin/psql/common.c:523-546](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/common.c#L523-L546)

## Overview
ClearOrSaveResult manages PGresult memory by either saving error results for later display via \errverbose or immediately clearing successful results.

## Definition

```c
static void
ClearOrSaveResult(PGresult *result)
```
## Detailed Description
ClearOrSaveResult implements psql's error result preservation mechanism. When a PGresult contains error information (PGRES_NONFATAL_ERROR or PGRES_FATAL_ERROR status), the function saves it to pset.last_error_result for potential later display by the \errverbose command, first clearing any previously saved error result to prevent memory leaks. For all other result statuses (successful operations), the function immediately calls PQclear() to free the result memory. This selective preservation allows users to examine detailed error information on demand while ensuring proper memory management for normal operations.

## Parameters / Member Variables
- `*result`: Pointer to the PGresult structure to be processed, may be NULL
## Dependencies
- Functions called/Symbols referenced:
  - [PQresultStatus](../P/PQresultStatus.md) (PostgreSQL libpq function)
  - [PQclear](../P/PQclear.md) (PostgreSQL libpq memory management function)
- [Result](../R/Result.md) status constants:
  - PGRES_NONFATAL_ERROR (PostgreSQL result status)
  - PGRES_FATAL_ERROR (PostgreSQL result status)
- Global variables accessed:
  - pset.last_error_result (psql global state for error preservation)
- Called from:
  - [ClearOrSaveAllResults](ClearOrSaveAllResults.md) (src/bin/psql/common.c:552)
  - [PSQLexec](../P/PSQLexec.md) (src/bin/psql/common.c:657)
  - [SendQuery](../S/SendQuery.md) (src/bin/psql/common.c:1143, 1146, 1161, 1164, 1229)
  - [DescribeQuery](../D/DescribeQuery.md) (src/bin/psql/common.c:1342, 1416)
  - [ExecQueryAndProcessResults](../E/ExecQueryAndProcessResults.md) (src/bin/psql/common.c:1549, 1696, 1738, 1803, 1809)

## Notes and Other Information
This function is central to psql's memory management strategy for query results. It implements the policy of preserving error results for debugging purposes while immediately freeing successful results to minimize memory usage. The function safely handles NULL results and ensures that only one error result is kept at a time by clearing any existing saved error before storing a new one. The preserved error results enable the \errverbose command to provide detailed error information even after subsequent queries have been executed. This applies to results from all queries, including internal "back door" queries used for debugging, making it a comprehensive error tracking mechanism.

## Simplified Source

```c
static void ClearOrSaveResult(PGresult *result) {
    if (result) {
        // Check if result contains an error
        switch (PQresultStatus(result)) {
            case PGRES_NONFATAL_ERROR:
            case PGRES_FATAL_ERROR:
                // Save error for \errverbose command
                PQclear(pset.last_error_result);
                pset.last_error_result = result;
                break;

            default:
                // Free successful results immediately
                PQclear(result);
                break;
        }
    }
}
```