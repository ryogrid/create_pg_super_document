# cmp_list_len_contents_asc

## Location
[src/backend/parser/parse_agg.c:1769-1802](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_agg.c#L1769-L1802)

## Overview
A two-level comparator function that sorts lists first by length and then by contents, ensuring deterministic ordering of grouping sets with identical lengths.

## Definition

```c
static int
cmp_list_len_contents_asc(const ListCell *a, const ListCell *b)
```
## Detailed Description
This function implements a hierarchical comparison strategy for lists of integers:

1. **Primary Sort**: First compares lists by length using cmp_list_len_asc, sorting shorter lists before longer ones.

2. **Secondary Sort**: When lists have equal length (primary comparison returns 0), it performs element-by-element comparison of the list contents. It iterates through both lists simultaneously and compares corresponding integer values:
   - Returns 1 if any element in list a is greater than the corresponding element in list b
   - Returns -1 if any element in list a is less than the corresponding element in list b
   - Continues comparison until a difference is found

3. **Deterministic Ordering**: This ensures that grouping sets of the same size have a consistent, predictable ordering based on their content, which is important for query plan consistency and result reproducibility.

The function is specifically designed for sorting lists of integers that represent grouping column references (ressortgroupref values).

## Parameters / Member Variables
- : ListCell pointer containing the first List of integers to compare
- : ListCell pointer containing the second List of integers to compare

## Dependencies
- Functions called/Symbols referenced:
  - [cmp_list_len_asc](cmp_list_len_asc.md): Primary length-based comparison
  - lfirst: Extracts List pointers from ListCells
  - forboth: PostgreSQL macro for parallel iteration over two lists
  - lfirst_int: Extracts integer values from ListCells
- Called from:
  - [expand_grouping_sets](../e/expand_grouping_sets.md): Uses this for final deterministic sorting of grouping combinations

## Notes and Other Information
- Implements a lexicographic ordering when lists have equal length
- Essential for ensuring deterministic query plans and consistent GROUPING SETS behavior
- Part of the PostgreSQL GROUPING SETS optimization that sorts combinations for efficient processing
- The integer comparison assumes the list elements are ressortgroupref values from GROUP BY expressions
- Uses PostgreSQL's standard list iteration macros for performance and safety

## Simplified Source

```c
static int
cmp_list_len_contents_asc(const ListCell *a, const ListCell *b)
{
    int res = cmp_list_len_asc(a, b);

    // If lengths are equal, compare contents element by element
    if (res == 0) {
        List *la = (List *) lfirst(a);
        List *lb = (List *) lfirst(b);
        ListCell *lca, *lcb;

        forboth(lca, la, lcb, lb) {
            int va = lfirst_int(lca);
            int vb = lfirst_int(lcb);

            if (va > vb)
                return 1;
            if (va < vb)
                return -1;
        }
    }

    return res;
}
```