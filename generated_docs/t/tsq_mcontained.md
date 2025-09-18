# tsq_mcontained

## Location
src/backend/utils/adt/tsquery_op.c: 354 - 359

## Overview
Implements the PostgreSQL text search query contained operator, determining if the first TSQuery is contained within the second TSQuery by reversing the arguments to tsq_mcontains.

## Definition
```c
Datum tsq_mcontained(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the <@ (contained by) operator for TSQuery objects. It provides a simple wrapper around tsq_mcontains by swapping the order of arguments, effectively reversing the containment relationship. When called as "A <@ B", it internally executes "B @> A" using DirectFunctionCall2 to invoke tsq_mcontains with the arguments in reverse order. This elegant implementation avoids code duplication while providing the semantically opposite operator.

## Parameters / Member Variables
- Function follows PostgreSQL's PG_FUNCTION_ARGS convention:
  - Argument 0: `query` - The TSQuery that should be contained
  - Argument 1: `container` - The TSQuery that should contain the terms

## Dependencies
- Functions called/Symbols referenced:
  - DirectFunctionCall2 (PostgreSQL utility to call another function directly)
  - [tsq_mcontains](tsq_mcontains.md) (the actual containment implementation with arguments swapped)
  - PG_RETURN_DATUM (PostgreSQL macro to return generic result)
  - PG_GETARG_DATUM (PostgreSQL macro to extract generic arguments)
- Types referenced:
  - Datum (PostgreSQL generic data type)
- Called from (representative examples):
  - No direct references found (typically called through PostgreSQL's operator dispatch system)

## Notes and Other Information
- This is a PostgreSQL SQL function accessible as the <@ operator for TSQuery types
- Provides the inverse operation of @> (contains) by simply swapping argument order
- The implementation demonstrates PostgreSQL's function call mechanism using DirectFunctionCall2
- Inherits all performance characteristics and behavior from tsq_mcontains
- Example: if query A contains query B, then B is contained by A (A @> B ≡ B <@ A)
- Part of PostgreSQL's full-text search functionality for complex query relationships