# right_merge_direction

## Location
[src/backend/optimizer/path/pathkeys.c:2100-2136](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/pathkeys.c#L2100-L2136)

## Overview
Determines whether a pathkey embodies the preferred sort direction for merging its target column by comparing it against query pathkeys.

## Definition

```c
static bool
right_merge_direction(PlannerInfo *root, PathKey *pathkey)
```
## Detailed Description
This function is used during merge join planning to determine the preferred sort direction for a given pathkey. It first searches through the query's ORDER BY pathkeys () to find a matching pathkey that has the same equivalence class and operator family. If a match is found, it returns true if the sort strategies match, indicating that this pathkey's direction aligns with the query's requirements. If no matching ORDER BY request is found, the function defaults to preferring the ascending direction (BTLessStrategyNumber).

The function plays a crucial role in optimizing merge joins by ensuring that the chosen sort direction minimizes additional sorting overhead while satisfying query requirements.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing query planning context and pathkey information
- `*pathkey`: The PathKey structure being evaluated for its sort direction preference
## Dependencies
- Functions called/Symbols referenced:
  - [PathKey](../P/PathKey.md) (structure type)
  - BTLessStrategyNumber (constant)
- Called from (representative examples):
  - [pathkeys_useful_for_merging](../p/pathkeys_useful_for_merging.md)

## Notes and Other Information
- The function ignores  when making comparisons, which means additional sorting might still be needed in some cases
- When no ORDER BY clause matches, the function defaults to preferring ascending order
- This is a static function used internally within the pathkeys.c module for merge join optimization

## Simplified Source

```c
static bool
right_merge_direction(PlannerInfo *root, PathKey *pathkey)
{
    ListCell *l;

    // Check if pathkey matches any query ORDER BY column
    foreach(l, root->query_pathkeys) {
        PathKey *query_pathkey = (PathKey *) lfirst(l);

        if (pathkey->pk_eclass == query_pathkey->pk_eclass &&
            pathkey->pk_opfamily == query_pathkey->pk_opfamily) {
            // Found matching column - prefer direction if strategies match
            // Note: ignores pk_nulls_first for simplicity
            return (pathkey->pk_strategy == query_pathkey->pk_strategy);
        }
    }

    // No matching ORDER BY request - default to ascending direction
    return (pathkey->pk_strategy == BTLessStrategyNumber);
}
```