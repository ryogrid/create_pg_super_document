# pg_get_keywords

## Location
src/backend/utils/adt/misc.c: 418 - 495

## Overview
Returns information about all SQL grammar keywords recognized by PostgreSQL, including their reservation status and label usage rules.

## Definition
```c
Datum pg_get_keywords(PG_FUNCTION_ARGS)
```

## Detailed Description
This set-returning function provides comprehensive information about PostgreSQL's SQL keywords. It iterates through the ScanKeywords table and returns details about each keyword including:

1. **Keyword name**: The actual keyword string
2. **Category code**: Single-character code indicating reservation level (U/C/T/R)  
3. **Bare label flag**: Boolean indicating if keyword can be used as a bare label
4. **Category description**: Human-readable description of the reservation level
5. **Label description**: Human-readable description of label usage rules

The function categorizes keywords into four types:
- **UNRESERVED_KEYWORD** (U): Can be used freely as identifiers
- **COL_NAME_KEYWORD** (C): Can be column names but not function/type names
- **TYPE_FUNC_NAME_KEYWORD** (T): Reserved but can still be function/type names  
- **RESERVED_KEYWORD** (R): Fully reserved, cannot be used as identifiers

The function also indicates whether each keyword can be used as a "bare label" (without AS keyword) in certain SQL contexts.

## Parameters / Member Variables
- No input parameters (void parameter list)

## Dependencies
- Functions called/Symbols referenced:
  - SRF_IS_FIRSTCALL
  - SRF_FIRSTCALL_INIT  
  - SRF_PERCALL_SETUP
  - SRF_RETURN_NEXT
  - SRF_RETURN_DONE
  - get_call_result_type
  - TupleDescGetAttInMetadata
  - GetScanKeyword
  - BuildTupleFromCStrings
  - HeapTupleGetDatum
  - unconstify
- Global data structures referenced:
  - ScanKeywords
  - ScanKeywordCategories  
  - ScanKeywordBareLabel
- Constants referenced:
  - UNRESERVED_KEYWORD
  - COL_NAME_KEYWORD
  - TYPE_FUNC_NAME_KEYWORD
  - RESERVED_KEYWORD
  - TYPEFUNC_COMPOSITE
- Called from:
  - SQL function calls (no direct C references found)

## Notes and Other Information
- This function powers the pg_get_keywords() SQL function used by tools like psql's \dS command
- The returned data helps applications determine safe identifier usage and keyword conflicts
- Uses the standard PostgreSQL SRF (Set Returning Function) framework for iterating through results
- The keyword data comes from the scanner's built-in keyword tables which are generated during build
- Internationalization support through gettext (_() macro) for description strings
- Memory management handled through SRF framework's multi-call context
- The 'bare label' concept relates to SQL syntax where some keywords can appear without AS in certain contexts