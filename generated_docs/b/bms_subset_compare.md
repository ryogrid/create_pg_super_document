# bms_subset_compare

## Location
[src/backend/nodes/bitmapset.c:445-509](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/bitmapset.c#L445-L509)

## Overview
Efficiently compares two bitmap sets to determine their subset/superset/equality relationship in a single operation, avoiding the need for multiple separate subset tests.

## Definition

```c
BMS_Comparison
bms_subset_compare(const Bitmapset *a, const Bitmapset *b)
```
## Detailed Description
This function performs a comprehensive comparison between two bitmap sets and returns one of four possible relationships: BMS_EQUAL (sets are identical), BMS_SUBSET1 (a is a subset of b), BMS_SUBSET2 (b is a subset of a), or BMS_DIFFERENT (neither is a subset of the other). The function is optimized to determine the relationship in a single pass through the bitmap words, making it more efficient than calling bms_is_subset twice. It handles NULL inputs by treating them as empty sets, and uses bitwise operations to detect bits present in one set but not the other.

## Parameters / Member Variables
- `a`: The first bitmap set to compare (can be NULL, representing an empty set)
- `b`: The second bitmap set to compare (can be NULL, representing an empty set)

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_valid_set](bms_is_valid_set.md) (validation function for bitmap sets)
  - BMS_Comparison (enum type for comparison results)
  - BMS_EQUAL, BMS_SUBSET1, BMS_SUBSET2, BMS_DIFFERENT (enum values)
  - bitmapword (type for bitmap word storage)
- Called from (representative examples):
  - [consider_index_join_outer_rels](../c/consider_index_join_outer_rels.md) (index path optimization)
  - [remove_useless_groupby_columns](../r/remove_useless_groupby_columns.md) (query optimization)
  - [set_cheapest](../s/set_cheapest.md) (path selection)
  - [add_path](../a/add_path.md) (path management)

## Notes and Other Information
The function uses a state-based approach where it starts assuming equality and updates the relationship as differences are discovered. The early termination logic ensures that as soon as it's determined that neither set is a subset of the other, the function returns BMS_DIFFERENT immediately. This function is particularly valuable in the query optimizer where comparing sets of relation IDs or other identifiers is common, and knowing the exact relationship helps make better optimization decisions.

## Simplified Source

```c
BMS_Comparison bms_subset_compare(const Bitmapset *a, const Bitmapset *b)
{
    // Handle NULL cases (empty sets)
    if (a == NULL)
        return (b == NULL) ? BMS_EQUAL : BMS_SUBSET1;
    if (b == NULL)
        return BMS_SUBSET2;

    // Compare common words and track relationship
    BMS_Comparison result = BMS_EQUAL;
    int shortlen = Min(a->nwords, b->nwords);

    for (int i = 0; i < shortlen; i++)
    {
        bitmapword aword = a->words[i];
        bitmapword bword = b->words[i];

        // Check if a has bits not in b
        if ((aword & ~bword) != 0)
        {
            if (result == BMS_SUBSET1)
                return BMS_DIFFERENT;  // Neither is subset of other
            result = BMS_SUBSET2;      // b is subset of a
        }

        // Check if b has bits not in a
        if ((bword & ~aword) != 0)
        {
            if (result == BMS_SUBSET2)
                return BMS_DIFFERENT;  // Neither is subset of other
            result = BMS_SUBSET1;      // a is subset of b
        }
    }

    // Handle different word lengths
    if (a->nwords > b->nwords)
    {
        // a has extra words, so a is not subset of b
        return (result == BMS_SUBSET1) ? BMS_DIFFERENT : BMS_SUBSET2;
    }
    else if (a->nwords < b->nwords)
    {
        // b has extra words, so b is not subset of a
        return (result == BMS_SUBSET2) ? BMS_DIFFERENT : BMS_SUBSET1;
    }

    return result;
}
```