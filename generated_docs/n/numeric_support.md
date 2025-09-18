# numeric_support

## Location
[src/backend/utils/adt/numeric.c:1194-1243](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L1194-L1243)

## Overview
This function serves as a planner support function for numeric type coercion operations, optimizing away unnecessary numeric() function calls that only increase precision without changing scale.

## Definition


## Detailed Description
The  function is a PostgreSQL planner support function specifically designed to optimize numeric type coercion operations during query planning. It identifies and eliminates redundant numeric() function calls that only represent increases in allowable precision while keeping the scale unchanged. The function examines cast operations between numeric types with different typmod (type modifier) constraints and determines whether the cast can be simplified to a simple relabeling operation rather than an actual data transformation. This optimization is important because precision increases don't require any actual computation on the data - they simply allow more digits to be stored. However, scale changes (which affect decimal positions) and constraints that reduce precision cannot be optimized away as they require actual data manipulation.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS macro which provides:
  - : A Node pointer containing the support request (typically SupportRequestSimplify)

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for type checking)
  - SupportRequestSimplify
  - FuncExpr
  - list_length
  - lsecond
  - linitial
  - exprTypmod
  - [DatumGetInt32](../D/DatumGetInt32.md)
  - [numeric_typmod_scale](numeric_typmod_scale.md)
  - [numeric_typmod_precision](numeric_typmod_precision.md)
  - [is_valid_numeric_typmod](../i/is_valid_numeric_typmod.md)
  - relabel_to_typmod
  - PG_RETURN_POINTER
- Called from:
  - Used as a support function registered in PostgreSQL's system catalogs for numeric type operations

## Notes and Other Information
- This is a PostgreSQL planner support function interface (uses PG_FUNCTION_ARGS/PG_RETURN_POINTER)
- Only optimizes precision increases with unchanged scale - scale changes require actual data transformation
- Helps improve query performance by eliminating unnecessary function calls during execution
- Part of PostgreSQL's query optimization infrastructure for numeric operations
- Located in src/backend/utils/adt/numeric.c:1194-1243
- Returns NULL if no optimization is possible, otherwise returns an optimized expression node
- Essential for efficient handling of numeric type casts in complex queries with multiple precision constraints