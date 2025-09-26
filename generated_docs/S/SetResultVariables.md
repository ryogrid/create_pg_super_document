# SetResultVariables

## Location
[src/bin/psql/common.c:461-500](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/common.c#L461-L500)

## Overview
SetResultVariables updates psql's special variables based on query execution results, providing status information and error details for user queries.

## Definition

```c
static void
SetResultVariables(PGresult *result, bool success)
```
## Detailed Description
SetResultVariables is a static function that maintains psql's special variables that track the status of user-entered queries. When a query succeeds, it sets ERROR to "false", SQLSTATE to "00000" (indicating no error), and ROW_COUNT to the number of affected/returned rows. When a query fails, it sets ERROR to "true", extracts the SQLSTATE code and primary error message from the result, sets ROW_COUNT to "0", and updates the LAST_ERROR_SQLSTATE and LAST_ERROR_MESSAGE variables for later reference. The function handles cases where SQLSTATE information may be unavailable (such as libpq-detected connection errors) by using empty strings as fallbacks.

## Parameters / Member Variables
- : Pointer to the PGresult structure containing query execution results and potential error information
- : Boolean indicating whether the query execution was successful

## Dependencies
- Functions called/Symbols referenced:
  - [PQcmdTuples](../P/PQcmdTuples.md) (PostgreSQL libpq function for row count)
  - [PQresultErrorField](../P/PQresultErrorField.md) (PostgreSQL libpq function for error details)
  - [SetVariable](SetVariable.md) (psql variable management function)
- Constants referenced:
  - PG_DIAG_SQLSTATE (PostgreSQL diagnostic field identifier)
  - PG_DIAG_MESSAGE_PRIMARY (PostgreSQL diagnostic field identifier)
- Global variables accessed:
  - pset.vars (psql variable storage)
- Called from:
  - [DescribeQuery](../D/DescribeQuery.md) (src/bin/psql/common.c:1341, 1415)
  - [ExecQueryAndProcessResults](../E/ExecQueryAndProcessResults.md) (src/bin/psql/common.c:1545, 1801)

## Notes and Other Information
This function implements psql's policy of tracking query status only for user-entered queries, not for internal slash commands. The special variables it manages (ERROR, SQLSTATE, ROW_COUNT, LAST_ERROR_SQLSTATE, LAST_ERROR_MESSAGE) are essential for script automation and conditional logic in psql. The function gracefully handles edge cases where error information may be incomplete, particularly for connection-level errors detected by libpq rather than the PostgreSQL server. The ROW_COUNT variable uses the result of PQcmdTuples, which returns affected rows for modification commands and returned rows for SELECT queries.