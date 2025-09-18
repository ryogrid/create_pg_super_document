# timestamp_smaller

## Location
src/backend/utils/adt/timestamp.c: 2756 - 2770

## Overview
Returns the smaller of two timestamp values, implementing the PostgreSQL LEAST() function for timestamp types.

## Definition


## Detailed Description
This function compares two timestamp values and returns the chronologically earlier (smaller) timestamp. It uses the internal timestamp comparison function to ensure consistency with other timestamp comparison operations. The function handles both finite timestamps and infinite values (TIMESTAMP_NOT_FINITE).

The function extracts two Timestamp arguments from the PostgreSQL function call interface, compares them using timestamp_cmp_internal(), and returns the smaller value. This is commonly used in SQL queries with the LEAST() function or in internal PostgreSQL operations requiring timestamp minimum calculations.

## Parameters / Member Variables
- : First timestamp value to compare (from PG_GETARG_TIMESTAMP(0))  
- : Second timestamp value to compare (from PG_GETARG_TIMESTAMP(1))
- : The smaller of the two input timestamps

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMP (PostgreSQL function call interface macro)
  - timestamp_cmp_internal (internal timestamp comparison function)
  - PG_RETURN_TIMESTAMP (PostgreSQL return value macro)
  - Timestamp (timestamp data type)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of the "Arithmetic" operators on date/times section in timestamp.c
- Uses timestamp_cmp_internal to ensure agreement with other comparison operations
- Handles all timestamp values including infinite timestamps
- Typically exposed as SQL function LEAST() for timestamp types
- Located at src/backend/utils/adt/timestamp.c:2756-2770