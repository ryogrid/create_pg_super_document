# pathkeys_useful_for_ordering

## Location
[src/backend/optimizer/path/pathkeys.c:2137-2166](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/pathkeys.c#L2137-L2166)

## Overview
Counts the number of pathkeys that are useful for meeting the query's requested output ordering, considering the possibility of incremental sort optimization.

## Definition

```c
static int
pathkeys_useful_for_ordering(PlannerInfo *root, List *pathkeys)
```
## Detailed Description
This function evaluates how many pathkeys from a given list are useful for satisfying the query's ORDER BY requirements. The function leverages the incremental sort capability in PostgreSQL, where a prefix list of keys can potentially improve the performance of the requested ordering even if not all keys match exactly. 

The function calls  to determine how many leading pathkeys from the query's  are contained within the provided pathkeys list. This count represents the number of pathkeys that can be leveraged to optimize the ordering operation, potentially allowing for incremental sorting rather than a full sort operation.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context including the query's pathkeys
- : List of PathKey structures to evaluate for ordering usefulness

## Dependencies
- Functions called/Symbols referenced:
  - [pathkeys_count_contained_in](pathkeys_count_contained_in.md) (function to count common pathkeys)
- Called from (representative examples):
  - [truncate_useless_pathkeys](../t/truncate_useless_pathkeys.md)

## Notes and Other Information
- The function takes advantage of PostgreSQL's incremental sort feature introduced to optimize partially ordered data
- Returns 0 if no valuable keys are found, otherwise returns the number of leading keys shared
- This is a static function used internally within the pathkeys.c module for path optimization
- The incremental sort optimization can significantly improve performance when dealing with large datasets that are already partially sorted

## Simplified Source

```c
static int
pathkeys_useful_for_ordering(PlannerInfo *root, List *pathkeys)
{
    int n_common_pathkeys;

    // Count how many leading pathkeys match the query's ordering requirements
    pathkeys_count_contained_in(root->query_pathkeys, pathkeys, &n_common_pathkeys);

    return n_common_pathkeys;
}
```