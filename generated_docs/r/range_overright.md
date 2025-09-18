# range_overright

## Location
[src/backend/utils/adt/rangetypes.c:956-971](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L956-L971)

## Overview
The range_overright function is a PostgreSQL built-in function that tests whether the first range does not extend to the left of the second range.

## Definition
```c
Datum range_overright(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the PostgreSQL range "does not extend to left of" operator (&> operator). It takes two range values as input and returns a boolean indicating whether the first range does not extend beyond the lower bound of the second range. The function serves as a wrapper that extracts the range arguments from the PostgreSQL function call context and delegates the actual comparison to the internal range_overright_internal function.

The function follows PostgreSQL's standard function interface pattern and is typically invoked through SQL queries using the &> operator.

## Parameters / Member Variables
- The function uses PostgreSQL's standard function calling convention with implicit parameters accessed via PG_GETARG_RANGE_P macros:
  - First parameter: RangeType pointer to the first range (r1)
  - Second parameter: RangeType pointer to the second range (r2)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_RANGE_P (to extract range arguments)
  - [range_get_typcache](range_get_typcache.md) (to get type cache information)
  - RangeTypeGetOid (to get the OID of the range type)
  - [range_overright_internal](range_overright_internal.md) (performs the actual overright test)
- Called from (representative examples):
  - No direct references found in the codebase (typically called via SQL operator &>)

## Notes and Other Information
- Located in src/backend/utils/adt/rangetypes.c:956-971
- This is the external interface for the range overright operation
- The actual comparison logic is implemented in range_overright_internal
- Used internally by PostgreSQL when the &> operator is applied to range types in SQL queries
- Returns false for empty ranges following the same semantics as the internal function
- Complementary to range_overleft, providing the opposite directional test