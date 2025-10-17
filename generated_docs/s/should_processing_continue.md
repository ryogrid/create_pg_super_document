# should_processing_continue

## Location
[src/bin/pg_amcheck/pg_amcheck.c:962-1005](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_amcheck/pg_amcheck.c#L962-L1005)

## Overview
This function analyzes PostgreSQL query result status to determine whether parallel slot processing should continue or be aborted based on the severity of errors encountered.

## Definition

```c
static bool
should_processing_continue(PGresult *res)
```
## Detailed Description
The should_processing_continue function provides intelligent error handling logic for pg_amcheck's parallel execution framework. It examines query results to distinguish between:

1. **Expected successful results** (PGRES_COMMAND_OK, PGRES_TUPLES_OK, PGRES_NONFATAL_ERROR) that allow processing to continue

2. **Expected but scrutinized errors** (PGRES_FATAL_ERROR) where it examines the severity field to determine if the error is:
   - Recoverable errors (like corruption reports from amcheck functions) that should not stop processing
   - Fatal errors (FATAL/PANIC severity) that indicate serious database problems requiring processing termination

3. **Unexpected result types** (bad responses, copy operations, pipeline states) that indicate protocol violations or unexpected server states requiring immediate termination

The function is designed to handle the fact that amcheck functions report corruption differently: heap checking returns corruption via result sets while btree checking uses ERROR messages, but both may encounter legitimate server errors that shouldn't halt the entire checking process.

## Parameters / Member Variables
- `*res`: PGresult pointer containing the result from an executed SQL query that needs to be evaluated for continuation decisions
## Dependencies
- Functions called/Symbols referenced:
  - [PQresultStatus](../P/PQresultStatus.md) (PostgreSQL libpq function to get result status)
  - [PQresultErrorField](../P/PQresultErrorField.md) (PostgreSQL libpq function to extract error field information)
  - PG_DIAG_SEVERITY_NONLOCALIZED (PostgreSQL diagnostic field constant)
  - Various PGRES_* constants (PostgreSQL result status constants)
- Called from (representative examples):
  - [verify_heap_slot_handler](../v/verify_heap_slot_handler.md) (in pg_amcheck.c:1101)
  - [verify_btree_slot_handler](../v/verify_btree_slot_handler.md) (in pg_amcheck.c:1170)

## Notes and Other Information
- The function is static, meaning it's only accessible within the pg_amcheck.c compilation unit
- Returns true to continue processing, false to abort further parallel operations
- Handles the asymmetry between heap corruption reporting (via result sets) and btree corruption reporting (via ERROR messages)
- Distinguishes between corruption-related errors (which are expected) and system-level failures (which require termination)
- Critical for maintaining robustness in parallel checking operations where one corrupted relation shouldn't prevent checking of other relations
- Located in src/bin/pg_amcheck/pg_amcheck.c:962-1005

## Simplified Source

```c
static bool
should_processing_continue(PGresult *res)
{
    const char *severity;

    switch (PQresultStatus(res))
    {
        // Expected successful results
        case PGRES_COMMAND_OK:
        case PGRES_TUPLES_OK:
        case PGRES_NONFATAL_ERROR:
            return true;

        // Check error severity for fatal errors
        case PGRES_FATAL_ERROR:
            severity = PQresultErrorField(res, PG_DIAG_SEVERITY_NONLOCALIZED);
            if (severity == NULL)
                return false;  // Lost connection
            if (strcmp(severity, "FATAL") == 0 || strcmp(severity, "PANIC") == 0)
                return false;  // Fatal database error
            return true;  // Recoverable error like corruption

        // Unexpected result types
        default:
            return false;
    }
}
```