# range_ne

## Location
src/backend/utils/adt/rangetypes.c: 625 - 637

## Overview
This PostgreSQL function implements the inequality operator (<>) for range types, providing the public interface for comparing two ranges for inequality.

## Definition


## Detailed Description
The  function serves as the PostgreSQL built-in function that implements the inequality operator (<>) for range types. It acts as a wrapper around the internal  function, handling the PostgreSQL function calling convention and argument extraction. The function takes two range arguments from the PostgreSQL function argument structure, obtains the appropriate type cache information, and delegates the actual comparison logic to . This separation allows the core inequality logic to be reused by other internal functions while providing a clean interface for SQL operations.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0: First range value (RangeType *) to compare
  - Argument 1: Second range value (RangeType *) to compare

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_RANGE_P
  - [range_get_typcache](range_get_typcache.md)
  - RangeTypeGetOid
  - [range_ne_internal](range_ne_internal.md)
  - PG_RETURN_BOOL
- Called from (representative examples):
  - No direct callers found (likely called through PostgreSQL's operator dispatch system)

## Notes and Other Information
- This function is typically invoked through SQL expressions using the <> or != operators (e.g., )
- The actual inequality logic is implemented in , making this function primarily a PostgreSQL function interface wrapper
- The function uses PostgreSQL's type cache system to handle different range types efficiently
- Returns a boolean Datum indicating whether the two ranges are not equal
- Located in src/backend/utils/adt/rangetypes.c:625-637