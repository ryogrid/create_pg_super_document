# bitne

## Location
[src/backend/utils/adt/varbit.c:865-888](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varbit.c#L865-L888)

## Overview
Inequality comparison operator for bit string data types that returns true if two bit strings differ in either content or length.

## Definition

```c
Datum
bitne(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements the SQL inequality operator (<> or !=) for bit string types (BIT and VARBIT). It performs a comprehensive inequality check that considers both the bit content and the exact length of the strings. The function is optimized with a fast path that immediately returns true if the bit lengths differ, avoiding the more expensive bit-by-bit comparison in such cases.

When the lengths are equal, the function delegates to the internal  function to perform the detailed comparison and returns true if the comparison result is non-zero. The function properly handles memory management for potentially toasted (compressed/out-of-line) bit string values, ensuring no memory leaks occur during the comparison process.

This function directly corresponds to the SQL  or  operator when used with BIT or VARBIT columns and is essential for WHERE clauses, JOIN conditions, and other SQL operations requiring bit string inequality testing.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments
  -  (VarBit*): First bit string operand extracted from function arguments
  -  (VarBit*): Second bit string operand extracted from function arguments

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract VarBit argument from function args
  - : Macro to get the bit length of a bit string
  - : Internal comparison function for detailed bit string comparison
  - : Macro to free memory if argument was a copy (detoasted)
  - : Macro to return a boolean result

- Called from (representative examples):
  - PostgreSQL SQL executor when processing inequality expressions
  - B-tree index operations for bit string types
  - [Query](../Q/Query.md) optimization and constraint checking

## Notes and Other Information
- Returns true if bit strings differ in either content or length
- Implements fast path optimization by checking lengths first before detailed comparison
- Properly handles memory management for toasted (compressed) bit string values
- Used by the PostgreSQL query executor for SQL  and  operations on BIT and VARBIT types
- Part of the complete set of comparison operators for bit string types
- Ensures exact inequality testing - trailing bits matter ("101" != "1010")
- Logically opposite of the  function
- Located in src/backend/utils/adt/varbit.c:865-888