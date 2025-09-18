# range_overleft

## Location
src/backend/utils/adt/rangetypes.c: 915 - 927

## Overview
The range_overleft function is a PostgreSQL built-in function that tests whether the first range does not extend to the right of the second range.

## Definition
```c
Datum range_overleft(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the PostgreSQL range "does not extend to right of" operator (&< operator). It takes two range values as input and returns a boolean indicating whether the first range does not extend beyond the upper bound of the second range. The function serves as a wrapper that extracts the range arguments from the PostgreSQL function call context and delegates the actual comparison to the internal range_overleft_internal function.

The function follows PostgreSQL's standard function interface pattern and is typically invoked through SQL queries using the &< operator.

## Parameters / Member Variables  
- The function uses PostgreSQL's standard function calling convention with implicit parameters accessed via PG_GETARG_RANGE_P macros:
  - First parameter: RangeType pointer to the first range (r1)
  - Second parameter: RangeType pointer to the second range (r2)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_RANGE_P (to extract range arguments)
  - [range_get_typcache](range_get_typcache.md) (to get type cache information)
  - RangeTypeGetOid (to get the OID of the range type)
  - [range_overleft_internal](range_overleft_internal.md) (performs the actual overleft test)
- Called from (representative examples):
  - No direct references found in the codebase (typically called via SQL operator &<)

## Notes and Other Information
- Located in src/backend/utils/adt/rangetypes.c:915-927
- This is the external interface for the range overleft operation
- The actual comparison logic is implemented in range_overleft_internal
- Used internally by PostgreSQL when the &< operator is applied to range types in SQL queries
- Returns false for empty ranges following the same semantics as the internal function