# bms_difference

## Location
[src/backend/nodes/bitmapset.c:346-411](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/bitmapset.c#L346-L411)

## Overview
Creates a new Bitmapset containing members from the first set that are not present in the second set (set difference operation).

## Definition

```c
Bitmapset *
bms_difference(const Bitmapset *a, const Bitmapset *b)
```
## Detailed Description
This function performs a bitwise difference operation (A - B) on two Bitmapsets, creating a new Bitmapset that contains only the bits that are set in the first set but not in the second set. The function includes several optimizations: it pre-checks if the result would be empty using bms_nonempty_difference() to avoid unnecessary allocation, handles cases where the first set has more words than the second efficiently, and trims trailing zero words when necessary. The operation uses bitwise AND with the complement of the second set (~b->words[i]).

## Parameters / Member Variables
- : First input bitmapset (minuend - what to subtract from, can be NULL)
- : Second input bitmapset (subtrahend - what to subtract, can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_valid_set](bms_is_valid_set.md) (validation macro for input bitmapsets)
  - [bms_copy](bms_copy.md) (creates a copy of a bitmapset)
  - [bms_nonempty_difference](bms_nonempty_difference.md) (checks if difference would be non-empty)

- Called from (representative examples):
  - [add_child_rel_equivalences](../a/add_child_rel_equivalences.md)
  - [check_index_predicates](../c/check_index_predicates.md)
  - [add_paths_to_joinrel](../a/add_paths_to_joinrel.md)
  - [have_unsafe_outer_join_ref](../h/have_unsafe_outer_join_ref.md)
  - [remove_useless_groupby_columns](../r/remove_useless_groupby_columns.md)
  - [finalize_plan](../f/finalize_plan.md)
  - [make_restrictinfo_internal](../m/make_restrictinfo_internal.md)
  - [pull_varnos_walker](../p/pull_varnos_walker.md)
  - [examine_variable](../e/examine_variable.md)

## Notes and Other Information
- Returns NULL if the first input is NULL (nothing to subtract from)
- Returns a copy of the first input if the second input is NULL (nothing to subtract)
- Optimizes for the common case of empty results by pre-checking with bms_nonempty_difference()
- Handles different word lengths efficiently: no trailing zero removal needed when 'a' has more words than 'b'
- When both sets have the same length or 'b' is longer, tracks the last non-zero word for trimming
- Uses bitwise AND with complement (~) operation to remove bits: result->words[i] &= ~b->words[i]
- The result is either NULL or a newly allocated Bitmapset that must be freed by the caller
- Essential for query optimization operations that need to exclude certain relations or parameters
- Widely used in join planning, equivalence class processing, and constraint analysis

## Simplified Source

```c
Bitmapset *
bms_difference(const Bitmapset *a, const Bitmapset *b)
{
    Bitmapset  *result;
    int         i;

    // Handle NULL cases
    if (a == NULL)
        return NULL;
    if (b == NULL)
        return bms_copy(a);

    // Optimization: check if result would be empty before allocation
    if (!bms_nonempty_difference(a, b))
        return NULL;

    // Copy the first set
    result = bms_copy(a);

    // Remove b's bits from result using bitwise AND with complement
    if (result->nwords > b->nwords)
    {
        // a is longer than b, no need to trim trailing zeros
        for (i = 0; i < b->nwords; i++)
        {
            result->words[i] &= ~b->words[i];  // Remove bits present in b
        }
    }
    else
    {
        // Same size or b is longer, may need to trim trailing zeros
        int lastnonzero = -1;

        for (i = 0; i < result->nwords; i++)
        {
            result->words[i] &= ~b->words[i];  // Remove bits present in b

            // Track last non-zero word for trimming
            if (result->words[i] != 0)
                lastnonzero = i;
        }

        // Trim trailing zero words
        result->nwords = lastnonzero + 1;
    }

    return result;
}
```