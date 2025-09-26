# ExecCleanTypeFromTL

## Location
[src/backend/executor/execTuples.c:2037-2042](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L2037-L2042)

## Overview
Generates a tuple descriptor for the result tuple of a target list, excluding resjunk columns from the result.

## Definition
```c
TupleDesc ExecCleanTypeFromTL(List *targetList)
```

## Detailed Description
This function creates a "clean" tuple descriptor from a target list by omitting resjunk columns from the result. It serves as a companion to ExecTypeFromTL, providing the same functionality but filtering out columns that are marked as resjunk (intermediate columns used during query processing that should not appear in the final result).

This function is particularly useful when creating tuple descriptors for final query results, portal outputs, or cached plan result descriptions where only the user-visible columns should be included. The "clean" aspect refers to removing the internal/temporary columns that were needed during query execution but are not part of the final output schema.

Like ExecTypeFromTL, it delegates the actual work to ExecTypeFromTLInternal, but passes true for the skipjunk parameter to indicate that resjunk columns should be filtered out.

## Parameters / Member Variables
- `targetList`: A List of TargetEntry nodes representing the target list from a parse or plan tree (must not be an ExprState target list)

## Dependencies
- Functions called/Symbols referenced:
  - [ExecTypeFromTLInternal](ExecTypeFromTLInternal.md) (the actual implementation function with skipjunk=true)
- Called from (representative examples):
  - [ExecInitJunkFilter](ExecInitJunkFilter.md)
  - [PortalStart](../P/PortalStart.md)
  - [PlanCacheComputeResultDesc](../P/PlanCacheComputeResultDesc.md)

## Notes and Other Information
- This function excludes resjunk columns, making it suitable for creating final result descriptors
- Commonly used in contexts where the tuple descriptor represents the schema visible to clients or external consumers
- The distinction from ExecTypeFromTL is crucial: this function creates "clean" descriptors while ExecTypeFromTL includes all columns
- Uses ExecTypeFromTLInternal with skipjunk=true to filter out intermediate columns
- Essential for proper result set formatting in portals and cached plan result descriptions
- The resulting TupleDesc represents only the columns that should be visible in the final query output