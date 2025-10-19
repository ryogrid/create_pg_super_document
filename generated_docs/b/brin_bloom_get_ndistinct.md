# brin_bloom_get_ndistinct

## Location
[src/backend/access/brin/brin_bloom.c:496-538](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_bloom.c#L496-L538)

## Overview
Determines the ndistinct value used to size bloom filter for BRIN (Block Range Index) bloom operators, adjusting the value based on the pages per range configuration.

## Definition

```c
static int
brin_bloom_get_ndistinct(BrinDesc *bdesc, BloomOptions *opts)
```
## Detailed Description
This function calculates the appropriate number of distinct values to use when sizing a bloom filter for BRIN indexes. It takes into account the pages per range value and applies several adjustments and safety constraints:

1. **Relative values**: If the ndistinct value is negative, it's treated as relative to the maximum number of tuples in the range
2. **Safety constraints**: Applies minimum and maximum bounds to prevent unreasonably small or large bloom filters
3. **Range-based calculation**: Uses MaxHeapTuplesPerPage multiplied by pagesPerRange to estimate maximum tuples

The function includes several safeguards to ensure the bloom filter is appropriately sized - not too small to be ineffective, and not too large to waste memory.

## Parameters / Member Variables
- `*bdesc`: BRIN descriptor containing index information, used to get pages per range
- `*opts`: Bloom filter options containing the base ndistinct value
## Dependencies
- Functions called/Symbols referenced:
  - BrinGetPagesPerRange
  - BloomGetNDistinctPerRange
  - BlockNumberIsValid
  - MaxHeapTuplesPerPage
  - BLOOM_MIN_NDISTINCT_PER_RANGE
- Called from (representative examples):
  - [brin_bloom_add_value](brin_bloom_add_value.md)

## Notes and Other Information
- The function contains several TODO comments (marked with XXX) suggesting potential improvements:
  - Better handling when pagesPerRange is not supplied
  - Using actual ndistinct estimates for columns
  - More accurate estimation of rows per BRIN range instead of using MaxHeapTuplesPerPage
- The calculation assumes each page gets MaxHeapTuplesPerPage tuples, which is noted as likely a significant over-estimate
- Negative ndistinct values are interpreted as percentages relative to the maximum possible tuples in the range

## Simplified Source

```c
static int
brin_bloom_get_ndistinct(BrinDesc *bdesc, BloomOptions *opts)
{
    // Get configuration values
    BlockNumber pagesPerRange = BrinGetPagesPerRange(bdesc->bd_index);
    double ndistinct = BloomGetNDistinctPerRange(opts);

    Assert(BlockNumberIsValid(pagesPerRange));

    // Calculate maximum tuples possible in the range
    double maxtuples = MaxHeapTuplesPerPage * pagesPerRange;

    // Handle negative values as relative to maximum tuples
    if (ndistinct < 0)
        ndistinct = (-ndistinct) * maxtuples;

    // Apply safety bounds: not too small, not larger than maximum possible
    ndistinct = Max(ndistinct, BLOOM_MIN_NDISTINCT_PER_RANGE);
    ndistinct = Min(ndistinct, maxtuples);

    return (int) ndistinct;
}
```