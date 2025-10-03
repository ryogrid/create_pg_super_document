# verify_btree_slot_handler

## Location
[src/bin/pg_amcheck/pg_amcheck.c:1118-1180](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_amcheck/pg_amcheck.c#L1118-L1180)

## Overview
A ParallelSlotHandler function that processes and displays results from btree index verification commands in the pg_amcheck utility.

## Definition

```c
static bool
verify_btree_slot_handler(PGresult *res, PGconn *conn, void *context)
```
## Detailed Description
The  function is a callback handler designed to process results from btree index checking operations. Unlike heap verification, btree checking functions are expected to return empty results on success (one void row or zero rows if skipped). The function primarily handles error conditions and version compatibility issues.

When btree verification succeeds, the function expects either zero rows (if the check was skipped due to object state) or one void row. If more than one row is returned, it indicates a potential version mismatch between pg_amcheck and the amcheck extension, triggering appropriate warnings.

For failed queries, the function formats and displays error messages with proper indentation, similar to the heap handler. It also handles progress display coordination to ensure clean output formatting when progress reporting is enabled.

## Parameters / Member Variables
- `*res`: PGresult pointer containing the query results from the btree verification command
- `*conn`: PGconn pointer to the database connection on which the query was executed
- `*context`: Void pointer to a RelationInfo structure containing information about the index being verified
## Dependencies
- Functions called/Symbols referenced:
  - [RelationInfo](../R/RelationInfo.md) (struct type)
  - [PQresultStatus](../P/PQresultStatus.md)
  - PGRES_TUPLES_OK
  - [PQntuples](../P/PQntuples.md)
  - fprintf (to stderr)
  - pg_log_warning
  - pg_log_warning_detail
  - pg_log_warning_hint
  - [indent_lines](../i/indent_lines.md)
  - [PQerrorMessage](../P/PQerrorMessage.md)
  - printf (with internationalization via _())
  - FREE_AND_SET_NULL
  - [should_processing_continue](../s/should_processing_continue.md)
- Called from:
  - [main](../m/main.md) (at src/bin/pg_amcheck/pg_amcheck.c:792)

## Notes and Other Information
- This is a static function, only accessible within pg_amcheck.c
- Sets the global variable  to false when errors are detected
- Handles progress display synchronization through  flag
- Issues warnings when btree functions return unexpected row counts, suggesting version incompatibility
- Uses structured logging functions (pg_log_warning, pg_log_warning_detail, pg_log_warning_hint) for better error reporting
- Manages memory cleanup for RelationInfo context fields
- Part of the parallel verification framework in pg_amcheck, specifically for btree index verification
- Expects btree checking functions to return void results, unlike heap functions which return detailed error information