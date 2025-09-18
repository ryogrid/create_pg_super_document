# should_processing_continue

## Location
src/bin/pg_amcheck/pg_amcheck.c: 962 - 1005

## Overview
This function analyzes PostgreSQL query result status to determine whether parallel slot processing should continue or be aborted based on the severity of errors encountered.

## Definition


## Detailed Description
The should_processing_continue function provides intelligent error handling logic for pg_amcheck's parallel execution framework. It examines query results to distinguish between:

1. **Expected successful results** (PGRES_COMMAND_OK, PGRES_TUPLES_OK, PGRES_NONFATAL_ERROR) that allow processing to continue

2. **Expected but scrutinized errors** (PGRES_FATAL_ERROR) where it examines the severity field to determine if the error is:
   - Recoverable errors (like corruption reports from amcheck functions) that should not stop processing
   - Fatal errors (FATAL/PANIC severity) that indicate serious database problems requiring processing termination

3. **Unexpected result types** (bad responses, copy operations, pipeline states) that indicate protocol violations or unexpected server states requiring immediate termination

The function is designed to handle the fact that amcheck functions report corruption differently: heap checking returns corruption via result sets while btree checking uses ERROR messages, but both may encounter legitimate server errors that shouldn't halt the entire checking process.

## Parameters / Member Variables
- : PGresult pointer containing the result from an executed SQL query that needs to be evaluated for continuation decisions

## Dependencies
- Functions called/Symbols referenced:
  - PQresultStatus (PostgreSQL libpq function to get result status)
  - PQresultErrorField (PostgreSQL libpq function to extract error field information)
  - PG_DIAG_SEVERITY_NONLOCALIZED (PostgreSQL diagnostic field constant)
  - Various PGRES_* constants (PostgreSQL result status constants)
- Called from (representative examples):
  - verify_heap_slot_handler (in pg_amcheck.c:1101)
  - verify_btree_slot_handler (in pg_amcheck.c:1170)

## Notes and Other Information
- The function is static, meaning it's only accessible within the pg_amcheck.c compilation unit
- Returns true to continue processing, false to abort further parallel operations
- Handles the asymmetry between heap corruption reporting (via result sets) and btree corruption reporting (via ERROR messages)
- Distinguishes between corruption-related errors (which are expected) and system-level failures (which require termination)
- Critical for maintaining robustness in parallel checking operations where one corrupted relation shouldn't prevent checking of other relations
- Located in src/bin/pg_amcheck/pg_amcheck.c:962-1005