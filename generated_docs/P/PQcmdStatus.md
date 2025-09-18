# PQcmdStatus

## Location
src/interfaces/libpq/fe-exec.c: 3752 - 3764

## Overview
PQcmdStatus retrieves the command status string from a PostgreSQL query result, indicating the type and outcome of the executed SQL command.

## Definition
```c
char *PQcmdStatus(PGresult *res)
```

## Detailed Description
PQcmdStatus returns a pointer to the command status string stored in a PGresult structure. This string contains information about the SQL command that was executed, such as "SELECT 5" for a SELECT statement that returned 5 rows, "INSERT 0 1" for a successful INSERT operation, "UPDATE 3" for an UPDATE that modified 3 rows, etc. The function provides a simple null-check and returns the cmdStatus field directly from the result structure.

## Parameters / Member Variables
- `res`: Pointer to a PGresult structure containing query results

## Dependencies
- Functions called/Symbols referenced:
  - (None - direct field access)
- Called from (representative examples):
  - [PrintQueryStatus](PrintQueryStatus.md) (src/bin/psql/common.c:960)
  - [ExecQueryAndProcessResults](../E/ExecQueryAndProcessResults.md) (src/bin/psql/common.c:1589)
  - ecpg_process_output (src/interfaces/ecpg/ecpglib/execute.c:1866)
  - [test_pipelined_insert](../t/test_pipelined_insert.md) (src/test/modules/libpq_pipeline/libpq_pipeline.c:1172, 1174)

## Notes and Other Information
- Returns NULL if the PGresult pointer is NULL
- The returned string is owned by the PGresult structure and should not be freed by the caller
- [Command](../C/Command.md) status strings follow PostgreSQL's standard format for different SQL command types
- Part of the libpq client interface for PostgreSQL database connectivity
- Commonly used for logging, debugging, and determining the success/impact of SQL operations