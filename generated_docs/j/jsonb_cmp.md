# jsonb_cmp

## Location
src/backend/utils/adt/jsonb_op.c: 236 - 252

## Overview
Compares two JSONB values and returns an integer indicating their relative order for sorting purposes.

## Definition
Datum jsonb_cmp(PG_FUNCTION_ARGS)

## Detailed Description
The jsonb_cmp function implements the comparison operator for JSONB data types, providing a three-way comparison result used for ordering operations like sorting and indexing. It returns a negative value if the first JSONB value is less than the second, zero if they are equal, and a positive value if the first value is greater than the second.

Like jsonb_eq, this function delegates the actual comparison logic to compareJsonbContainers, but instead of converting the result to a boolean, it directly returns the integer comparison result. This enables JSONB values to be used in ordered data structures such as B-tree indexes.

## Parameters / Member Variables
- : First JSONB value to compare (jba)
- : Second JSONB value to compare (jbb)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_JSONB_P
  - compareJsonbContainers (src/backend/utils/adt/jsonb_util.c:191-340)
  - PG_FREE_IF_COPY
  - PG_RETURN_INT32
- Data types used:
  - Jsonb

## Notes and Other Information
- Location: src/backend/utils/adt/jsonb_op.c:236-252
- This function serves as the backend implementation for JSONB comparison operators (<, >, <=, >=)
- The return value follows standard comparison semantics: negative (first < second), zero (equal), positive (first > second)
- Essential for JSONB indexing operations and ORDER BY clauses involving JSONB columns
- Memory management is handled with proper cleanup using PG_FREE_IF_COPY
- The comparison order is determined by the compareJsonbContainers function's implementation logic