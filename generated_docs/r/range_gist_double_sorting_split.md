# range_gist_double_sorting_split

## Location
[src/backend/utils/adt/rangetypes_gist.c:1318-1620](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_gist.c#L1318-L1620)

## Overview
An advanced GiST splitting algorithm that uses double sorting to minimize overlap between left and right groups by analyzing both lower and upper bounds of ranges.

## Definition
static void range_gist_double_sorting_split(TypeCacheEntry *typcache, GistEntryVector *entryvec, GIST_SPLITVEC *v)

## Detailed Description
This function implements the most sophisticated splitting strategy for GiST range indexes, based on the "double sorting-based node splitting algorithm for R-tree" by A. Korotkov. The algorithm aims to minimize overlap between the resulting left and right groups while maintaining acceptable distribution ratios.

The algorithm works in several phases:
1. **Preparation**: Creates two sorted arrays of range bounds - one sorted by lower bounds, another by upper bounds
2. **Split candidate evaluation**: Considers multiple split points by iterating through possible boundaries and evaluating each with 
3. **Entry distribution**: Distributes entries into three categories: definitely left, definitely right, and "common" entries that could go to either side
4. **Common entry resolution**: Uses delta calculations to optimally distribute common entries based on their proximity to group boundaries

The algorithm considers splits where the left group has an upper bound and the right group has a lower bound, trying to minimize overlap while ensuring both groups contain a reasonable number of entries. Common entries are distributed using subtype difference calculations when available, falling back to equal distribution otherwise.

## Parameters / Member Variables
- : Type cache entry containing range type information and comparison functions
- : Vector containing all the index entries to be split
- : Output structure that will contain the split result, including left/right entries and union ranges

## Dependencies
- Functions called/Symbols referenced:
  - [range_deserialize](range_deserialize.md)
  - qsort_arg
  - [interval_cmp_lower](../i/interval_cmp_lower.md)
  - [interval_cmp_upper](../i/interval_cmp_upper.md)
  - [range_cmp_bounds](range_cmp_bounds.md)
  - [range_gist_consider_split](range_gist_consider_split.md)
  - [range_gist_fallback_split](range_gist_fallback_split.md)
  - [call_subtype_diff](../c/call_subtype_diff.md)
  - [common_entry_cmp](../c/common_entry_cmp.md)
  - [DatumGetRangeTypeP](../D/DatumGetRangeTypeP.md)
  - PLACE_LEFT (macro)
  - PLACE_RIGHT (macro)
  - FirstOffsetNumber (constant)
  - OffsetNumberNext
  - [ConsiderSplitContext](../C/ConsiderSplitContext.md) (struct)
  - [NonEmptyRange](../N/NonEmptyRange.md) (struct)
  - CommonEntry (struct)
  - RangeBound (struct)
- Called from (representative examples):
  - [range_gist_picksplit](range_gist_picksplit.md)

## Notes and Other Information
- This is a static function, only accessible within the rangetypes_gist.c file
- Most sophisticated of the range splitting algorithms, providing optimal spatial organization
- Falls back to range_gist_fallback_split if no acceptable split is found
- Uses subtype difference functions when available for more precise delta calculations
- Implements the LIMIT_RATIO constraint to ensure balanced splits
- The algorithm is based on academic research and provides near-optimal index organization
- Handles degenerate cases by falling back to simpler splitting methods
- Common entries are sorted by delta values to distribute the most ambiguous entries first
- The two-phase iteration (lower bounds first, then upper bounds) ensures all possible optimal splits are considered
- Memory allocation for temporary arrays (by_lower, by_upper, common_entries) is done upfront for efficiency

## Simplified Source

```c
static void
range_gist_double_sorting_split(TypeCacheEntry *typcache,
                               GistEntryVector *entryvec,
                               GIST_SPLITVEC *v)
{
    ConsiderSplitContext context;
    OffsetNumber i, maxoff;
    RangeType  *left_range = NULL, *right_range = NULL;
    int common_entries_count;
    NonEmptyRange *by_lower, *by_upper;
    CommonEntry *common_entries;
    int nentries, i1, i2;
    RangeBound *right_lower, *left_upper;

    // Initialize split evaluation context
    memset(&context, 0, sizeof(ConsiderSplitContext));
    context.typcache = typcache;
    context.has_subtype_diff = OidIsValid(typcache->rng_subdiff_finfo.fn_oid);

    maxoff = entryvec->n - 1;
    nentries = context.entries_count = maxoff - FirstOffsetNumber + 1;
    context.first = true;

    // Allocate arrays for sorted range bounds
    by_lower = (NonEmptyRange *) palloc(nentries * sizeof(NonEmptyRange));
    by_upper = (NonEmptyRange *) palloc(nentries * sizeof(NonEmptyRange));

    // Extract bounds from each range
    for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
    {
        RangeType *range = DatumGetRangeTypeP(entryvec->vector[i].key);
        bool empty;

        range_deserialize(typcache, range,
                         &by_lower[i - FirstOffsetNumber].lower,
                         &by_lower[i - FirstOffsetNumber].upper,
                         &empty);
        Assert(!empty);
    }

    // Create two sorted arrays: by lower bound and by upper bound
    memcpy(by_upper, by_lower, nentries * sizeof(NonEmptyRange));
    qsort_arg(by_lower, nentries, sizeof(NonEmptyRange),
              interval_cmp_lower, typcache);
    qsort_arg(by_upper, nentries, sizeof(NonEmptyRange),
              interval_cmp_upper, typcache);

    // Phase 1: Find optimal split by iterating through lower bounds
    i1 = 0;
    i2 = 0;
    right_lower = &by_lower[i1].lower;
    left_upper = &by_upper[i2].lower;

    while (true)
    {
        // Find next unique lower bound for right group
        while (i1 < nentries &&
               range_cmp_bounds(typcache, right_lower, &by_lower[i1].lower) == 0)
        {
            if (range_cmp_bounds(typcache, &by_lower[i1].upper, left_upper) > 0)
                left_upper = &by_lower[i1].upper;
            i1++;
        }
        if (i1 >= nentries) break;
        right_lower = &by_lower[i1].lower;

        // Count entries that must go to left group
        while (i2 < nentries &&
               range_cmp_bounds(typcache, &by_upper[i2].upper, left_upper) <= 0)
            i2++;

        // Evaluate this split candidate
        range_gist_consider_split(&context, right_lower, i1, left_upper, i2);
    }

    // Phase 2: Find optimal split by iterating through upper bounds
    i1 = nentries - 1;
    i2 = nentries - 1;
    right_lower = &by_lower[i1].upper;
    left_upper = &by_upper[i2].upper;

    while (true)
    {
        // Find next unique upper bound for left group
        while (i2 >= 0 &&
               range_cmp_bounds(typcache, left_upper, &by_upper[i2].upper) == 0)
        {
            if (range_cmp_bounds(typcache, &by_upper[i2].lower, right_lower) < 0)
                right_lower = &by_upper[i2].lower;
            i2--;
        }
        if (i2 < 0) break;
        left_upper = &by_upper[i2].upper;

        // Count entries that must go to right group
        while (i1 >= 0 &&
               range_cmp_bounds(typcache, &by_lower[i1].lower, right_lower) >= 0)
            i1--;

        // Evaluate this split candidate
        range_gist_consider_split(&context, right_lower, i1 + 1, left_upper, i2 + 1);
    }

    // Fall back to simple split if no good split found
    if (context.first)
    {
        range_gist_fallback_split(typcache, entryvec, v);
        return;
    }

    // Allocate result vectors
    v->spl_left = (OffsetNumber *) palloc(nentries * sizeof(OffsetNumber));
    v->spl_right = (OffsetNumber *) palloc(nentries * sizeof(OffsetNumber));
    v->spl_nleft = 0;
    v->spl_nright = 0;

    // Allocate array for common entries
    common_entries_count = 0;
    common_entries = (CommonEntry *) palloc(nentries * sizeof(CommonEntry));

    // Distribute entries: left only, right only, or common
    for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
    {
        RangeType *range = DatumGetRangeTypeP(entryvec->vector[i].key);
        RangeBound lower, upper;
        bool empty;

        range_deserialize(typcache, range, &lower, &upper, &empty);

        if (range_cmp_bounds(typcache, &upper, context.left_upper) <= 0)
        {
            // Fits in left group
            if (range_cmp_bounds(typcache, &lower, context.right_lower) >= 0)
            {
                // Also fits in right group - it's a common entry
                common_entries[common_entries_count].index = i;
                if (context.has_subtype_diff)
                {
                    // Calculate delta for optimal placement
                    common_entries[common_entries_count].delta =
                        call_subtype_diff(typcache, lower.val, context.right_lower->val) -
                        call_subtype_diff(typcache, context.left_upper->val, upper.val);
                }
                else
                {
                    common_entries[common_entries_count].delta = 0;
                }
                common_entries_count++;
            }
            else
            {
                // Only fits in left group
                PLACE_LEFT(range, i);
            }
        }
        else
        {
            // Must fit in right group
            Assert(range_cmp_bounds(typcache, &lower, context.right_lower) >= 0);
            PLACE_RIGHT(range, i);
        }
    }

    // Distribute common entries based on calculated deltas
    if (common_entries_count > 0)
    {
        // Sort by delta to distribute most ambiguous entries first
        qsort(common_entries, common_entries_count, sizeof(CommonEntry), common_entry_cmp);

        // Distribute according to context.common_left threshold
        for (i = 0; i < common_entries_count; i++)
        {
            RangeType *range = DatumGetRangeTypeP(entryvec->vector[common_entries[i].index].key);

            if (i < context.common_left)
                PLACE_LEFT(range, common_entries[i].index);
            else
                PLACE_RIGHT(range, common_entries[i].index);
        }
    }

    v->spl_ldatum = PointerGetDatum(left_range);
    v->spl_rdatum = PointerGetDatum(right_range);
}
```