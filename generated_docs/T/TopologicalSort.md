# TopologicalSort

## Location
[src/backend/parser/parse_cte.c:863-914](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_cte.c#L863-L914)

## Overview
TopologicalSort performs a standard topological sorting operation on CTE (Common Table Expression) items to arrange them in dependency order, ensuring that each CTE appears before any CTE that depends on it.

## Definition
```c
static void TopologicalSort(ParseState *pstate, CteItem *items, int numitems)
```

## Detailed Description
This function implements a classic topological sort algorithm specifically designed for ordering Common Table Expressions based on their dependencies. The algorithm works by:

1. **Iterative Selection**: For each position in the final sequence, it scans the remaining unprocessed items to find one with no remaining dependencies (empty dependency bitmap)

2. **Cycle Detection**: If no item without dependencies is found, this indicates a circular dependency, which PostgreSQL does not support for mutual recursion between WITH items

3. **Item Arrangement**: When a dependency-free item is found, it's moved to the current position in the sorted sequence

4. **Dependency Update**: The sorted item's ID is removed from all remaining items' dependency bitmaps, potentially making other items dependency-free in subsequent iterations

The algorithm ensures that the final arrangement allows each CTE to be evaluated in an order where all its dependencies have already been processed. This is crucial for proper CTE resolution and execution planning.

## Parameters / Member Variables
- `pstate`: Parse state context used for error reporting and location information
- `items`: Array of CteItem structures representing the CTEs to be sorted
- `numitems`: Number of items in the items array

## Dependencies
- Functions called/Symbols referenced:
  - bms_is_empty (check if dependency bitmap is empty)
  - [bms_del_member](../b/bms_del_member.md) (remove item ID from dependency bitmap)
  - ereport (error reporting)
  - [errcode](../e/errcode.md) (error code specification)
  - [errmsg](../e/errmsg.md) (error message formatting)
  - [parser_errposition](../p/parser_errposition.md) (parse location for error reporting)
  - [CteItem](../C/CteItem.md) (CTE item structure)

- Called from:
  - [makeDependencyGraph](../m/makeDependencyGraph.md) (main dependency analysis function)

## Notes and Other Information
- The algorithm has O(n²) time complexity in the worst case, which is acceptable for typical numbers of CTEs in a WITH clause
- PostgreSQL does not support mutual recursion between different WITH items, so cycle detection results in an error rather than attempting resolution
- The topological sort is stable - items with no dependencies relative to each other maintain their original relative order
- The dependency tracking uses bitmaps (Bitmapset) for efficient set operations
- Error reporting includes the parse location of the problematic CTE for better user diagnostics
- This function modifies the items array in-place, reordering the CteItem structures