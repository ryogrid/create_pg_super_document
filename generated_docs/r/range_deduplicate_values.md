# range_deduplicate_values

## Location
[src/backend/access/brin/brin_minmax_multi.c:516-575](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L516-L575)

## Overview
range_deduplicate_values is an optimization function that removes duplicate values from the unsorted portion of a Ranges structure, improving storage efficiency and query performance in BRIN minmax-multi indexes.

## Definition

```c
static void
range_deduplicate_values(Ranges *range)
```
## Detailed Description
This function performs in-place deduplication of values in the unsorted portion of a Ranges structure. It serves as a lightweight optimization strategy that improves storage efficiency without the computational overhead of more expensive range consolidation operations.

The function operates through a multi-step process:

1. **Early termination**: If all values are already sorted (nsorted == nvalues), the function returns immediately
2. **Sorting**: Sorts all values (both previously sorted and unsorted) using qsort_arg with the appropriate comparison context
3. **Deduplication**: Performs a single-pass deduplication by comparing consecutive values and compacting the array
4. **State update**: Updates the Ranges structure to reflect that all values are now sorted and deduplicated
5. **Validation**: Calls AssertCheckRanges to verify structural integrity

The function is designed to be more efficient than full range consolidation because it avoids calling potentially expensive distance functions and doesn't attempt to merge values into ranges. It assumes that values don't duplicate with existing ranges since this is checked before values are added.

## Parameters / Member Variables
- : Pointer to the Ranges structure to deduplicate

## Dependencies
- Functions called/Symbols referenced:
  - qsort_arg
  - [compare_values](../c/compare_values.md)
  - [AssertCheckRanges](../A/AssertCheckRanges.md)
- Data structures referenced:
  - [Ranges](../R/Ranges.md)
  - [compare_context](../c/compare_context.md)
- Called from (representative examples):
  - [brin_range_serialize](../b/brin_range_serialize.md)
  - [ensure_free_space_in_buffer](../e/ensure_free_space_in_buffer.md)

## Notes and Other Information
- Operates only on the values portion, leaving range boundaries untouched
- Uses in-place deduplication to minimize memory usage
- The function includes a comment about potential future optimization using merge sort to leverage pre-sorted portions
- Critical for maintaining storage efficiency in BRIN minmax-multi indexes
- Assumes values don't duplicate with existing ranges due to pre-insertion validation
- Located in src/backend/access/brin/brin_minmax_multi.c:516-575
- Updates both nvalues and nsorted to reflect the new state after deduplication

## Simplified Source

```c
static void
range_deduplicate_values(Ranges *range)
{
    int start;
    compare_context cxt;

    // Early exit if all values are already sorted
    if (range->nsorted == range->nvalues)
        return;

    // Set up comparison context
    cxt.colloid = range->colloid;
    cxt.cmpFn = range->cmp;

    // Values start after the range boundaries
    start = 2 * range->nranges;

    // Sort all values (including previously sorted ones)
    qsort_arg(&range->values[start],
              range->nvalues, sizeof(Datum),
              compare_values, &cxt);

    // Deduplicate by compacting array
    int n = 1;
    for (int i = 1; i < range->nvalues; i++)
    {
        // Skip duplicate values
        if (compare_values(&range->values[start + i - 1],
                          &range->values[start + i],
                          (void *) &cxt) == 0)
            continue;

        // Keep unique value
        range->values[start + n] = range->values[start + i];
        n++;
    }

    // Update counts - all values are now sorted and deduplicated
    range->nvalues = n;
    range->nsorted = n;

    AssertCheckRanges(range, range->cmp, range->colloid);
}
```