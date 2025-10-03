# path_usage_comparator

## Location
[src/backend/optimizer/path/indxpath.c:1493-1525](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L1493-L1525)

## Overview
A qsort comparator function used to sort PathClauseUsage structures in increasing order of index access cost, with selectivity as a secondary sort criterion.

## Definition

```c
static int
path_usage_comparator(const void *a, const void *b)
```
## Detailed Description
This static comparator function is used by qsort to order PathClauseUsage entries based on their associated bitmap tree node costs. It first compares the access costs of two PathClauseUsage structures, and if the costs are equal, it uses selectivity as a tiebreaker. The function implements a standard three-way comparison returning -1, 0, or 1 to indicate the relative ordering of the two elements.

The function extracts cost and selectivity information by calling cost_bitmap_tree_node() on the path member of each PathClauseUsage structure, then performs the comparison logic to determine the proper sort order.

## Parameters / Member Variables
- `*a`: Pointer to the first PathClauseUsage pointer being compared
- `*b`: Pointer to the second PathClauseUsage pointer being compared
## Dependencies
- Functions called/Symbols referenced:
  - [cost_bitmap_tree_node](../c/cost_bitmap_tree_node.md)
  - PathClauseUsage
  - Cost
  - Selectivity
- Called from (representative examples):
  - [choose_bitmap_and](../c/choose_bitmap_and.md)

## Notes and Other Information
- This is a static function local to indxpath.c
- Used specifically for sorting PathClauseUsage arrays to optimize bitmap index scan planning
- The comparison prioritizes cost over selectivity, which aligns with PostgreSQL's cost-based optimization strategy
- Returns standard qsort comparator values: -1 (a < b), 0 (a == b), 1 (a > b)

## Simplified Source

```c
static int
path_usage_comparator(const void *a, const void *b)
{
    PathClauseUsage *pa = *(PathClauseUsage *const *) a;
    PathClauseUsage *pb = *(PathClauseUsage *const *) b;
    Cost acost, bcost;
    Selectivity aselec, bselec;

    // Get cost and selectivity for both paths
    cost_bitmap_tree_node(pa->path, &acost, &aselec);
    cost_bitmap_tree_node(pb->path, &bcost, &bselec);

    // Primary comparison: sort by cost
    if (acost < bcost)
        return -1;
    if (acost > bcost)
        return 1;

    // Secondary comparison: sort by selectivity if costs are equal
    if (aselec < bselec)
        return -1;
    if (aselec > bselec)
        return 1;

    return 0; // equal
}
```