# hashcostestimate

## Location
[src/backend/utils/adt/selfuncs.c:7197-7238](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L7197-L7238)

## Overview
A cost estimation function for hash index access paths that leverages generic cost estimation while accounting for hash index specific characteristics and limitations.

## Definition

```c
void
hashcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
				 Cost *indexStartupCost, Cost *indexTotalCost,
				 Selectivity *indexSelectivity, double *indexCorrelation,
				 double *indexPages)
```
## Detailed Description
The  function provides cost estimation for hash index scans by primarily delegating to the generic cost estimation framework while acknowledging the unique properties of hash indexes.

Hash indexes have fundamentally different access patterns compared to tree-based indexes:
- **Direct Access**: Hash indexes can go directly to the target bucket after computing the hash value, eliminating tree descent costs
- **Uniform Distribution**: The hash access method ensures buckets average one page in size
- **No Ordering**: Hash indexes provide no inherent ordering, so correlation is inherited from generic estimation (typically 0.0)

The function currently uses the generic cost model without hash-specific adjustments, though the comments identify several potential areas for improvement:
- Bucket-specific page costs (difficult without knowing target bucket)
- Hash collision modeling (currently not implemented)
- Hash comparison costs (considered minimal compared to general datatype comparisons)

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing global planning context and statistics
- `*path`: IndexPath structure describing the specific hash index access path being costed
- `loop_count`: Expected number of times this index scan will be executed (for nested loops)
- `*indexStartupCost`: Output parameter for one-time startup cost of the index scan
- `*indexTotalCost`: Output parameter for total cost including per-tuple processing
- `*indexSelectivity`: Output parameter for estimated fraction of table rows that will be returned
- `*indexCorrelation`: Output parameter for correlation between index and table ordering (typically 0.0 for hash)
- `*indexPages`: Output parameter for estimated number of index pages to be accessed
## Dependencies
- Functions called/Symbols referenced:
  - [genericcostestimate](../g/genericcostestimate.md)
- Called from (representative examples):
  - [hashhandler](hashhandler.md) (Hash access method handler)

## Notes and Other Information
- Currently uses the generic cost model without hash-specific modifications
- Does not charge for tree descent costs since hash indexes provide direct bucket access
- Comments identify potential improvements for bucket-specific costing and collision modeling
- [Hash](../H/Hash.md) value comparisons are considered much cheaper than general datatype comparisons
- The one-page-per-bucket guarantee simplifies some cost calculations
- Lossy operator effects from hash collisions are acknowledged but not currently modeled
- Future enhancements might include collision probability estimation and bucket distribution analysis

## Simplified Source

```c
void hashcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
                     Cost *indexStartupCost, Cost *indexTotalCost,
                     Selectivity *indexSelectivity, double *indexCorrelation,
                     double *indexPages)
{
    GenericCosts costs = {0};

    // Use generic cost estimation as baseline
    genericcostestimate(root, path, loop_count, &costs);

    // Hash indexes have no descent costs - direct bucket access after hash computation
    // Current implementation uses generic costs without hash-specific adjustments
    // Potential future improvements:
    // - Model bucket-specific page costs
    // - Account for hash collisions
    // - Adjust for hash comparison vs general datatype comparison costs

    // Return generic cost estimates
    *indexStartupCost = costs.indexStartupCost;
    *indexTotalCost = costs.indexTotalCost;
    *indexSelectivity = costs.indexSelectivity;
    *indexCorrelation = costs.indexCorrelation;  // Typically 0.0 for hash indexes
    *indexPages = costs.numIndexPages;
}
```