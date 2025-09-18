# list_member_int

## Location
src/backend/nodes/list.c: 702 - 721

## Overview
Tests whether a given integer value is a member of an integer list using direct integer comparison for equality determination.

## Definition
```c
bool list_member_int(const List *list, int datum)
```

## Detailed Description
The `list_member_int` function performs membership testing on PostgreSQL's List data structure specifically for integer lists. It iterates through the list cells using the `foreach` macro and compares each cell's integer value with the target datum using direct integer comparison (`==` operator). The function includes assertions to ensure the input list is actually an integer list type and validates list invariants for debugging purposes.

This function is optimized for integer comparison and should only be used with lists that contain integer values. It uses the `lfirst_int` macro to extract integer values from list cells, which is type-safe for integer lists.

## Parameters / Member Variables
- `list`: A constant pointer to the List structure to search within. Must be an integer list type.
- `datum`: An integer value representing the target value to search for in the list.

## Dependencies
- Functions called/Symbols referenced:
  - IsIntegerList - Validates that the list contains integer values
  - [check_list_invariants](../c/check_list_invariants.md) - Performs debugging validation of list structure
  - foreach - Macro for iterating through list cells
  - lfirst_int - Macro for accessing the integer value of a list cell

- Called from (representative examples):
  - [CopyGetAttnums](../C/CopyGetAttnums.md) - Used in COPY command processing
  - [BeginCopyFrom](../B/BeginCopyFrom.md)/BeginCopyTo - Used in COPY operations
  - ExecRelationIsTargetRelation - Used in executor utilities
  - [list_union_int](list_union_int.md) - Used when creating union of integer lists
  - [list_intersection_int](list_intersection_int.md) - Used when computing intersection of integer lists
  - [reorder_grouping_sets](../r/reorder_grouping_sets.md) - Used in query planning for grouping sets
  - [parseCheckAggregates](../p/parseCheckAggregates.md) - Used in aggregate function parsing

## Notes and Other Information
- The function uses direct integer value comparison
- Only suitable for integer lists; will assert if used with other list types
- Returns `true` if the integer is found, `false` otherwise
- Part of PostgreSQL's generic List API located in src/backend/nodes/list.c
- Commonly used throughout the system for checking membership in collections of attribute numbers, column identifiers, and other integer values
- Type-safe alternative to generic list membership functions when working with integers