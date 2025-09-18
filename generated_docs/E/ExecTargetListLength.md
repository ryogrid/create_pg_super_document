# ExecTargetListLength

## Location
src/backend/executor/execUtils.c: 1109 - 1118

## Overview
ExecTargetListLength returns the total number of items in a target list, including any resjunk (result junk) items that are used internally but not part of the final result.

## Definition
int ExecTargetListLength(List *targetlist)

## Detailed Description
This is a simple utility function that provides the count of target list entries. Despite its straightforward implementation as a wrapper around list_length(), it serves an important abstraction role in the executor. The function explicitly includes resjunk items in its count, which are target list entries marked for internal use (such as sort keys, join keys, or other working values) that don't appear in the final query result.

The comment indicates this function "used to be more complex, but fjoins are dead", referring to the removal of a legacy join implementation that required more sophisticated target list length calculation. This historical context explains why a dedicated function exists for what is now a simple operation.

## Parameters / Member Variables
- `targetlist`: List of TargetEntry nodes to count

## Dependencies
- Functions called/Symbols referenced:
  - list_length (get list length)
- Called from (representative examples):
  - [ExecTypeFromTLInternal](ExecTypeFromTLInternal.md) (tuple type construction from target list)
  - exec_rt_fetch (runtime tuple access)

## Notes and Other Information
The function includes resjunk items in its count, which is important for callers that need to allocate structures (like tuple descriptors) that must account for all target list entries, not just the visible result columns. This differs from functions that might only count non-resjunk entries for result set sizing.