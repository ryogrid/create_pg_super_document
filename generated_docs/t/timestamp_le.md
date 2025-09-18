# timestamp_le

## Location
src/backend/utils/adt/timestamp.c: 2252 - 2260

## Overview
Compares two timestamp values and returns true if the first timestamp is less than or equal to the second timestamp.

## Definition
Datum timestamp_le(PG_FUNCTION_ARGS)

## Detailed Description
This function implements the less-than-or-equal-to comparison operator (<=) for PostgreSQL timestamp values. It extracts two timestamp arguments from the function call arguments using PostgreSQL function argument macros, then delegates the actual comparison logic to timestamp_cmp_internal and returns true if the comparison result is less than or equal to 0.

## Parameters / Member Variables
- Function follows PostgreSQL's function calling convention using PG_FUNCTION_ARGS
- Argument 0: First timestamp value to compare
- Argument 1: Second timestamp value to compare

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMP (macro to extract timestamp arguments)
  - [timestamp_cmp_internal](timestamp_cmp_internal.md) (internal comparison function)
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- This function is part of PostgreSQL's SQL operator implementation for timestamp comparison
- Uses the internal comparison function for consistent comparison logic across all timestamp operators
- Returns a Datum (PostgreSQL's generic return type) containing a boolean value
- Located in src/backend/utils/adt/timestamp.c:2252-2260