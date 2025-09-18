# MergeScanSelCache

## Location
src/include/nodes/pathnodes.h: 2734 - 2746

## Overview
MergeScanSelCache is a caching structure that stores the results of expensive mergejoinscansel() calculations for specific sort orderings, improving performance when planning large join trees by avoiding redundant selectivity computations.

## Definition
```c
typedef struct MergeScanSelCache
{
    /* Ordering details (cache lookup key) */
    Oid         opfamily;       /* btree opfamily defining the ordering */
    Oid         collation;      /* collation for the ordering */
    int         strategy;       /* sort direction (ASC or DESC) */
    bool        nulls_first;    /* do NULLs come before normal values? */
    /* Results */
    Selectivity leftstartsel;   /* first-join fraction for clause left side */
    Selectivity leftendsel;     /* last-join fraction for clause left side */
    Selectivity rightstartsel;  /* first-join fraction for clause right side */
    Selectivity rightendsel;    /* last-join fraction for clause right side */
} MergeScanSelCache;
```

## Detailed Description
MergeScanSelCache provides a performance optimization for PostgreSQL's merge join cost estimation by caching the results of mergejoinscansel() function calls. The mergejoinscansel() function performs expensive statistical analysis to estimate how much of each input stream will be read during a merge join under different sort orderings. Since this analysis can be computationally intensive and the same calculations might be needed multiple times during join tree planning, the cache stores results keyed by specific ordering parameters (operator family, collation, strategy, and null positioning). Each cache entry contains four selectivity values representing the expected scan fractions for both the start and end positions of both join inputs.

## Parameters / Member Variables
- `opfamily`: OID of the B-tree operator family that defines the sort ordering used as cache lookup key
- `collation`: OID of the collation used for the ordering, part of the cache lookup key
- `strategy`: Sort strategy indicating direction (BTLessStrategyNumber for ASC, BTGreaterStrategyNumber for DESC)
- `nulls_first`: Boolean flag indicating whether NULL values are positioned before normal values in the sort order
- `leftstartsel`: Selectivity estimate for the fraction of the left input that must be scanned before finding the first matching join pair
- `leftendsel`: Selectivity estimate for the fraction of the left input that will be scanned before the join terminates
- `rightstartsel`: Selectivity estimate for the fraction of the right input that must be scanned before finding the first matching join pair
- `rightendsel`: Selectivity estimate for the fraction of the right input that will be scanned before the join terminates

## Dependencies
- Functions called/Symbols referenced:
  - Oid (for opfamily and collation identifiers)
  - Selectivity (for result fractions)
- Called from (representative examples):
  - [cached_scansel](../c/cached_scansel.md) (costsize.c:3996)
  - [initial_cost_mergejoin](../i/initial_cost_mergejoin.md) (costsize.c:3560)
  - [final_cost_mergejoin](../f/final_cost_mergejoin.md) (costsize.c:3993)

## Notes and Other Information
- Cache entries are stored in the scansel_cache list of mergejoinable RestrictInfo nodes
- The cache lookup is performed by comparing all four key fields (opfamily, collation, strategy, nulls_first) with the PathKey parameters
- Cache entries are allocated in the planner's memory context to ensure they persist throughout the planning process
- The cached selectivity values are computed by mergejoinscansel() which analyzes variable statistics to estimate merge join scan ranges
- This optimization is particularly beneficial for complex queries with multiple potential join orderings where the same merge join selectivity calculations would otherwise be repeated
- The cache is reset when RestrictInfo nodes are copied, ensuring fresh calculations for different query contexts
- Start selectivities represent startup costs while end selectivities represent total scan costs for merge join operations