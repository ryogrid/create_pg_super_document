# get_record_type_from_query

## Location
src/backend/utils/adt/jsonfuncs.c: 3660 - 3696

## Overview
A static function that determines the record type from the calling query context when the target type cannot be extracted from function arguments, particularly for  functions.

## Definition


## Detailed Description
This function extracts record type information from the SQL query context when it cannot be determined from function arguments. It's primarily used for  functions and as a fallback for  functions when the first argument is a null record. The function validates that the result type is composite and sets up the necessary tuple descriptor cache.

Key behaviors:
- Uses query context to determine result type structure
- Validates that the result type is composite (not domain-over-composite)
- Prevents memory leaks by cleaning up previous tuple descriptors
- Creates a copy of the tuple descriptor in the function's memory context
- Provides helpful error messages with usage hints

## Parameters / Member Variables
- : Function call information containing query context and execution details
- : Name of the calling function (used in error messages for clarity)
- : PopulateRecordCache structure to be populated with type information

## Dependencies
- Functions called/Symbols referenced:
  - get_call_result_type
  - TYPEFUNC_COMPOSITE
  - FreeTupleDesc
  - CreateTupleDescCopy
  - MemoryContextSwitchTo
  - ereport (for error handling)
- Called from (representative examples):
  - populate_record_worker
  - populate_recordset_worker

## Notes and Other Information
- This function is used when type information cannot be extracted from arguments
- Handles the case where the first argument is 
- Cannot handle domain-over-composite types due to syntactic limitations
- Includes memory management to prevent leaks on repeated calls
- Provides user-friendly error messages with hints for proper usage
- Part of PostgreSQL's JSON-to-record conversion infrastructure
- The error hint suggests using column definition lists in FROM clauses