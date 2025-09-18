# network_subset_support

## Location
src/backend/utils/adt/network.c: 981 - 1027

## Overview
Provides planner support for network subset/superset operators by converting operator and function calls to index conditions for query optimization.

## Definition
```c
Datum network_subset_support(PG_FUNCTION_ARGS)
```

## Detailed Description
The `network_subset_support` function is a planner support function that helps PostgreSQL's query optimizer convert network subset/superset operations into index conditions that can be efficiently executed using indexes. This function is part of PostgreSQL's support function infrastructure that allows custom data types to provide optimization hints to the planner.

The function processes `SupportRequestIndexCondition` requests, which are used by the planner to ask for index-optimizable versions of expressions. When it receives such a request, it examines the node type:

1. For operator expressions (`OpExpr`), it extracts the operator's arguments and delegates to `match_network_function`
2. For function expressions (`FuncExpr`), it similarly extracts the function's arguments and calls `match_network_function`

The `match_network_function` helper is responsible for analyzing the specific network operation and determining if it can be converted to an index-scannable condition.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `rawreq`: A Node pointer to the support request, typically a `SupportRequestIndexCondition`

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER (extract pointer argument)
  - IsA (type checking macro)
  - [is_opclause](../i/is_opclause.md) (check if node is operator expression)
  - [is_funcclause](../i/is_funcclause.md) (check if node is function expression) 
  - list_length (get list length)
  - linitial (get first list element)
  - lsecond (get second list element)
  - [match_network_function](../m/match_network_function.md) (convert network operations to index conditions)
  - Assert (assertion macro)
  - PG_RETURN_POINTER (return pointer result)
- Called from (representative examples):
  - No direct references found (likely called by PostgreSQL's planner support infrastructure)

## Notes and Other Information
- This is a support function for PostgreSQL's query planner optimization system
- Enables index-based execution of network subset/superset operations
- Works with both operator expressions (like >>=, <<=) and direct function calls
- The actual optimization logic is delegated to the `match_network_function` helper
- Returns NULL if the request cannot be optimized into an index condition
- Part of the broader PostgreSQL infrastructure for custom data type optimization
- Registered as a support function for network operators in the system catalogs