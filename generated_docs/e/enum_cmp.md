# enum_cmp

## Location
[src/backend/utils/adt/enum.c:378-391](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/enum.c#L378-L391)

## Overview
PostgreSQL function that implements the three-way comparison operation for enum data types, returning an integer indicating the relative ordering between two enum values.

## Definition
```c
Datum enum_cmp(PG_FUNCTION_ARGS)
```

## Detailed Description
The `enum_cmp` function provides a comprehensive comparison operation for PostgreSQL enum data types, implementing the standard three-way comparison semantics. It extracts two enum OID arguments and delegates to `enum_cmp_internal` to perform the actual comparison logic. The function returns an integer value that indicates the relationship between the two enum values: negative if the first is "smaller" (appears earlier in the enum type definition), zero if they are equal, and positive if the first is "larger" (appears later in the enum type definition).

This function is fundamental to PostgreSQL's type system infrastructure for enums, serving as the basis for all ordering operations including sorting, indexing, and range operations. It provides the canonical comparison interface that other enum operations can build upon.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - First argument (index 0): OID of the first enum value to compare
  - Second argument (index 1): OID of the second enum value to compare

## Dependencies
- Functions called/Symbols referenced:
  - [enum_cmp_internal](enum_cmp_internal.md): Core enum comparison function that performs the actual comparison logic
  - PG_GETARG_OID: Macro to extract OID arguments from function call
  - PG_RETURN_INT32: Macro to return a 32-bit integer result
- Called from (representative examples):
  - B-tree index operations for enum columns
  - ORDER BY clauses involving enum types
  - Sorting algorithms and merge operations

## Notes and Other Information
- Returns a signed 32-bit integer following standard comparison conventions:
  - Negative value: first enum < second enum
  - Zero: first enum = second enum  
  - Positive value: first enum > second enum
- Essential for implementing efficient indexing and sorting of enum columns
- Used internally by PostgreSQL's query planner for optimization decisions
- Forms the foundation for all other enum comparison operators (=, <, >, <=, >=)
- The actual comparison logic is centralized in `enum_cmp_internal` for consistency
- Part of the btree operator class for enum types, enabling B-tree indexing
- The function follows PostgreSQL's fmgr (function manager) calling convention