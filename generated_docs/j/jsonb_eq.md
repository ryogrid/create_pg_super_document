# jsonb_eq

## Location
src/backend/utils/adt/jsonb_op.c: 222 - 235

## Overview
Tests equality between two JSONB values, returning true if they are equal, false otherwise.

## Definition
Datum jsonb_eq(PG_FUNCTION_ARGS)

## Detailed Description
The jsonb_eq function implements the equality operator (=) for JSONB data types. It compares two JSONB values for structural and semantic equality by leveraging the compareJsonbContainers function. The function handles proper memory management by freeing copied arguments when necessary and returns a boolean result indicating whether the two JSONB values are identical.

The comparison is performed at the container level, comparing the root containers of both JSONB values. This ensures a deep comparison that considers the structure, data types, and values of all nested elements.

## Parameters / Member Variables
- : First JSONB value to compare (jba)
- : Second JSONB value to compare (jbb)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_JSONB_P
  - compareJsonbContainers (src/backend/utils/adt/jsonb_util.c:191-340)
  - PG_FREE_IF_COPY
  - PG_RETURN_BOOL
- Data types used:
  - Jsonb

## Notes and Other Information
- Location: src/backend/utils/adt/jsonb_op.c:222-235
- This function serves as the backend implementation for the JSONB equality operator
- Memory management is handled properly with PG_FREE_IF_COPY calls to prevent memory leaks
- The actual comparison logic is delegated to compareJsonbContainers, which performs deep structural comparison
- Returns true (1) when compareJsonbContainers returns 0, indicating the containers are equal