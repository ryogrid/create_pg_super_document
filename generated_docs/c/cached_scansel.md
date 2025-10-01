cached_scansel

## Overview
Provides cached execution of mergejoinscansel() to avoid redundant selectivity calculations for merge join operations with the same pathkey characteristics.

## Definition
```c
static MergeScanSelCache *cached_scansel(PlannerInfo *root, RestrictInfo *rinfo, PathKey *pathkey)
```

## Detailed Description
This function implements a caching mechanism for merge join scan selectivity calculations. It first searches through the cached results stored in rinfo->scansel_cache to see if selectivity has already been computed for the given pathkey characteristics. If a matching cache entry is found, it returns the cached result immediately.

If no cached result exists, it calls mergejoinscansel() to compute the selectivity estimates for both left and right sides of the merge join operation, then creates a new cache entry with these results. The cache entry is stored in the planners long-lived memory context to ensure it persists throughout the planning process.

The cache lookup is based on four key pathkey characteristics: operator family, collation, strategy number, and nulls_first ordering. This ensures that cached results are only reused when the merge operation characteristics are identical.

## Parameters / Member Variables
- `root`: PlannerInfo containing the query planning context
- `rinfo`: RestrictInfo clause for which selectivity is being computed
- `pathkey`: PathKey defining the sort ordering and merge characteristics

## Dependencies
- Functions called/Symbols referenced:
  - [mergejoinscansel](../m/mergejoinscansel.md)
  - [MergeScanSelCache](../M/MergeScanSelCache.md) (struct)
  - [PathKey](../P/PathKey.md) (struct)
- Called from (representative examples):
  - [initial_cost_mergejoin](../i/initial_cost_mergejoin.md)
  - cost_qual_eval_context

## Notes and Other Information
- Uses the restrictinfos scansel_cache list to store and retrieve cached selectivity results
- Cache entries are allocated in the planners memory context for persistence
- The cache key consists of opfamily, collation, strategy, and nulls_first from the pathkey
- Returns selectivity estimates for both start and end positions on left and right sides
- Helps optimize planning performance by avoiding redundant selectivity calculations for similar merge operations

## Simplified Source

```c
static MergeScanSelCache *cached_scansel(PlannerInfo *root, RestrictInfo *rinfo, PathKey *pathkey) {
    MergeScanSelCache *cache;
    ListCell *lc;

    // Check if we already have cached results for this pathkey
    foreach(lc, rinfo->scansel_cache) {
        cache = (MergeScanSelCache *) lfirst(lc);
        if (cache->opfamily == pathkey->pk_opfamily &&
            cache->collation == pathkey->pk_eclass->ec_collation &&
            cache->strategy == pathkey->pk_strategy &&
            cache->nulls_first == pathkey->pk_nulls_first)
            return cache;
    }

    // Not cached, compute selectivity estimates
    Selectivity leftstartsel, leftendsel, rightstartsel, rightendsel;
    mergejoinscansel(root,
                     (Node *) rinfo->clause,
                     pathkey->pk_opfamily,
                     pathkey->pk_strategy,
                     pathkey->pk_nulls_first,
                     &leftstartsel, &leftendsel,
                     &rightstartsel, &rightendsel);

    // Create and populate cache entry in planner context
    MemoryContext oldcontext = MemoryContextSwitchTo(root->planner_cxt);

    cache = (MergeScanSelCache *) palloc(sizeof(MergeScanSelCache));
    cache->opfamily = pathkey->pk_opfamily;
    cache->collation = pathkey->pk_eclass->ec_collation;
    cache->strategy = pathkey->pk_strategy;
    cache->nulls_first = pathkey->pk_nulls_first;
    cache->leftstartsel = leftstartsel;
    cache->leftendsel = leftendsel;
    cache->rightstartsel = rightstartsel;
    cache->rightendsel = rightendsel;

    // Add to cache and restore memory context
    rinfo->scansel_cache = lappend(rinfo->scansel_cache, cache);
    MemoryContextSwitchTo(oldcontext);

    return cache;
}
```