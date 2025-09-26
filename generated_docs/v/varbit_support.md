# varbit_support

## Location
[src/backend/utils/adt/varbit.c:702-741](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varbit.c#L702-L741)

## Overview
Provides planner support for the varbit() length coercion function by optimizing calls that don't require actual data truncation.

## Definition
```c
Datum varbit_support(PG_FUNCTION_ARGS)
```

## Detailed Description
The `varbit_support` function serves as a planner support function for PostgreSQL's query optimizer, specifically targeting the varbit() length coercion function. This function is part of PostgreSQL's support function framework that allows type-specific functions to provide optimization hints to the query planner.

The primary optimization implemented is flattening calls where the new maximum length is greater than or equal to the previous maximum length. In such cases, no actual data truncation is needed, so the function can eliminate the varbit() call entirely and replace it with a simple type relabeling operation. This optimization reduces runtime overhead by avoiding unnecessary function calls when the length constraint is already satisfied.

The function processes SupportRequestSimplify requests, examining the function call's arguments to determine if the optimization can be applied. It ignores the isExplicit parameter since the optimization is valid regardless of whether the cast is explicit or implicit.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `rawreq`: Support request node obtained via `PG_GETARG_POINTER(0)`

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro)
  - [SupportRequestSimplify](../S/SupportRequestSimplify.md)
  - [FuncExpr](../F/FuncExpr.md)
  - lsecond
  - [DatumGetInt32](../D/DatumGetInt32.md)
  - [exprTypmod](../e/exprTypmod.md)
  - [relabel_to_typmod](../r/relabel_to_typmod.md)
- Called from:
  - No direct callers found (called by PostgreSQL's planner system)

## Notes and Other Information
- Part of PostgreSQL's support function framework for query optimization
- Only handles SupportRequestSimplify request types
- Treats typmod 0 as invalid, consistent with varbit() function behavior
- The optimization eliminates unnecessary runtime function calls when length constraints are already satisfied
- Uses relabel_to_typmod for efficient type relabeling without data copying
- Ignores isExplicit argument as the optimization applies to both explicit and implicit casts
- Returns NULL when no optimization is possible, allowing normal function execution
- Located in src/backend/utils/adt/varbit.c:702-741