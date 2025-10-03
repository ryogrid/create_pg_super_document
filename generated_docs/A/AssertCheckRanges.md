# AssertCheckRanges

## Location
[src/backend/access/brin/brin_minmax_multi.c:296-425](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L296-L425)

## Overview
AssertCheckRanges is a comprehensive debugging function that validates the internal consistency and invariants of a Ranges structure used in BRIN minmax-multi indexes.

## Definition

```c
static void
AssertCheckRanges(Ranges *ranges, FmgrInfo *cmpFn, Oid colloid)
```
## Detailed Description
This function performs extensive validation of a Ranges structure, which is a core data structure in BRIN minmax-multi access method. The function verifies multiple critical invariants:

1. **Basic sanity checks**: Validates that counts are non-negative and relationships between nranges, nsorted, nvalues, and maxvalues are correct
2. **Range ordering**: Ensures that range boundary values are strictly ordered using AssertArrayOrder
3. **Point value ordering**: Validates that sorted single-point values maintain proper order
4. **Range coverage**: Verifies that no individual values fall within existing ranges (which would be redundant)
5. **Sorted vs unsorted separation**: Ensures values in the unsorted part don't duplicate values in the sorted part

The function uses binary search to efficiently check whether individual values are covered by existing ranges, and employs sophisticated validation logic to maintain the integrity of the compressed range representation.

## Parameters / Member Variables
- `*ranges`: Pointer to the Ranges structure to validate
- `*cmpFn`: FmgrInfo pointer to the comparison function for ordering operations
- `colloid`: OID of the collation to use for comparison operations
## Dependencies
- Functions called/Symbols referenced:
  - [AssertArrayOrder](AssertArrayOrder.md)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
  - [bsearch_arg](../b/bsearch_arg.md)
  - [compare_values](../c/compare_values.md)
- Data structures referenced:
  - [Ranges](../R/Ranges.md)
  - [compare_context](../c/compare_context.md)
- Called from (representative examples):
  - [range_deduplicate_values](../r/range_deduplicate_values.md)
  - [ensure_free_space_in_buffer](../e/ensure_free_space_in_buffer.md)
  - [range_add_value](../r/range_add_value.md)
  - [compactify_ranges](../c/compactify_ranges.md)

## Notes and Other Information
- This function only executes when USE_ASSERT_CHECKING is defined (debug builds)
- Implements sophisticated binary search logic to verify range coverage efficiently
- Critical for maintaining data integrity in BRIN minmax-multi indexes
- The function assumes ranges are stored as pairs of boundary values followed by individual point values
- Part of the comprehensive validation framework for BRIN index structures
- Located in src/backend/access/brin/brin_minmax_multi.c:296-425

## Simplified Source

```c
static void
AssertCheckRanges(Ranges *ranges, FmgrInfo *cmpFn, Oid colloid)
{
#ifdef USE_ASSERT_CHECKING
    // Basic sanity checks on range structure
    Assert(ranges->nranges >= 0);
    Assert(ranges->nsorted >= 0);
    Assert(ranges->nvalues >= ranges->nsorted);
    Assert(ranges->maxvalues >= 2 * ranges->nranges + ranges->nvalues);

    // Check ordering of range boundaries (2*nranges values)
    AssertArrayOrder(cmpFn, colloid, ranges->values, 2 * ranges->nranges);

    // Check ordering of sorted single-point values
    AssertArrayOrder(cmpFn, colloid, &ranges->values[2 * ranges->nranges],
                     ranges->nsorted);

    // Verify no values are covered by existing ranges
    if (ranges->nranges > 0)
    {
        for (int i = 0; i < ranges->nvalues; i++)
        {
            Datum value = ranges->values[2 * ranges->nranges + i];
            Datum minvalue = ranges->values[0];
            Datum maxvalue = ranges->values[2 * ranges->nranges - 1];

            // Skip values outside overall range bounds
            if (DatumGetBool(FunctionCall2Coll(cmpFn, colloid, value, minvalue)) ||
                DatumGetBool(FunctionCall2Coll(cmpFn, colloid, maxvalue, value)))
                continue;

            // Binary search to ensure value doesn't fall within any range
            int start = 0, end = ranges->nranges - 1;
            while (start <= end)
            {
                int midpoint = (start + end) / 2;
                Datum rangemin = ranges->values[2 * midpoint];
                Datum rangemax = ranges->values[2 * midpoint + 1];

                if (DatumGetBool(FunctionCall2Coll(cmpFn, colloid, value, rangemin)))
                    end = midpoint - 1;
                else if (DatumGetBool(FunctionCall2Coll(cmpFn, colloid, rangemax, value)))
                    start = midpoint + 1;
                else
                    Assert(false);  // Value should not be within any range
            }
        }
    }

    // Check unsorted values don't duplicate sorted values
    if (ranges->nsorted > 0)
    {
        compare_context cxt;
        cxt.colloid = ranges->colloid;
        cxt.cmpFn = ranges->cmp;

        for (int i = ranges->nsorted; i < ranges->nvalues; i++)
        {
            Datum value = ranges->values[2 * ranges->nranges + i];
            Assert(bsearch_arg(&value, &ranges->values[2 * ranges->nranges],
                              ranges->nsorted, sizeof(Datum),
                              compare_values, (void *) &cxt) == NULL);
        }
    }
#endif
}
```