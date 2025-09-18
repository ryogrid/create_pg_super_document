# verify_heap_slot_handler

## Location
src/bin/pg_amcheck/pg_amcheck.c: 1037 - 1117

## Overview
A ParallelSlotHandler function that processes and displays results from heap table verification commands in the pg_amcheck utility.

## Definition


## Detailed Description
The  function is a callback handler that processes query results from heap table checking operations. It formats and displays verification results, including any corruption issues found in heap tables. The function handles different levels of detail in error reporting, from table-level issues down to specific block, offset, and attribute-level problems.

When verification errors are found, it outputs detailed location information (database, schema, table, block number, tuple offset, and attribute number as applicable) along with descriptive error messages. For successful queries with no errors, the function simply processes the empty result set. For failed queries, it formats and displays the error message with proper indentation.

The function also manages memory cleanup for the RelationInfo context and determines whether parallel processing should continue based on the result status.

## Parameters / Member Variables
- : PGresult pointer containing the query results from the heap verification command
- : PGconn pointer to the database connection on which the query was executed
- : Void pointer to a RelationInfo structure containing information about the table being verified

## Dependencies
- Functions called/Symbols referenced:
  - RelationInfo (struct type)
  - PQresultStatus
  - PGRES_TUPLES_OK
  - PQntuples
  - PQgetisnull
  - PQgetvalue
  - printf (with internationalization via _())
  - indent_lines
  - PQerrorMessage
  - FREE_AND_SET_NULL
  - should_processing_continue
- Called from:
  - main (at src/bin/pg_amcheck/pg_amcheck.c:776)

## Notes and Other Information
- This is a static function, only accessible within pg_amcheck.c
- Sets the global variable  to false when errors are detected
- Handles four different levels of error detail: attribute-level, tuple-level, block-level, and table-level
- Uses internationalized error messages via the _() macro
- Properly manages memory by freeing allocated strings in the RelationInfo context
- The function returns a boolean indicating whether parallel processing should continue
- Part of the parallel verification framework in pg_amcheck