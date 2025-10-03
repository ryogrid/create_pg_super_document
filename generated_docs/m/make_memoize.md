# make_memoize

## Location
[src/backend/optimizer/plan/createplan.c:6569-6593](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L6569-L6593)

## Overview
Creates a Memoize plan node that caches the results of its child plan based on parameter values, optimizing performance for repeated executions with the same parameters.

## Definition

```c
static Memoize *
make_memoize(Plan *lefttree, Oid *hashoperators, Oid *collations,
			 List *param_exprs, bool singlerow, bool binary_mode,
			 uint32 est_entries, Bitmapset *keyparamids)
```
## Detailed Description
This function constructs a Memoize plan node, which implements result caching for subplans that are executed multiple times with different parameter values. The Memoize node maintains a hash table where keys are formed from the parameter expressions and values are the cached results. This optimization is particularly effective for parameterized nested loop joins where the inner plan is repeatedly executed with different outer values.

The function initializes all the memoization-specific fields including the hash operators for key comparison, collation information, parameter expressions that form the cache key, and various control flags and estimates.

## Parameters / Member Variables
- `*lefttree`: The input Plan node whose results will be cached
- `*hashoperators`: Array of hash operators for the memoization keys
- `*collations`: Array of collations for the memoization keys
- `*param_exprs`: List of parameter expressions that form the cache key
- `singlerow`: Boolean indicating if the child plan produces at most one row per parameter combination
- `binary_mode`: Boolean indicating if binary comparison mode should be used
- `est_entries`: Estimated number of cache entries
- `*keyparamids`: Bitmapset identifying which parameters are part of the cache key
## Dependencies
- Functions called/Symbols referenced:
  - [Memoize](../M/Memoize.md) (struct type, created with makeNode())
  - [Agg](../A/Agg.md) (related type)
- Called from (representative examples):
  - [create_memoize_plan](../c/create_memoize_plan.md)

## Notes and Other Information
- This is a static function, accessible only within the same source file
- The Memoize node has no right child (righttree is set to NULL)  
- No additional qualification conditions are applied (qual is set to NIL)
- The target list is directly copied from the child plan
- The numKeys field is automatically calculated from the length of param_exprs
- Memoization is most effective when the same parameter values are likely to be repeated
- Located in src/backend/optimizer/plan/createplan.c at lines 6569-6593

## Simplified Source

```c
// Simplified version of make_memoize
static Memoize *make_memoize(Plan *lefttree, Oid *hashoperators, Oid *collations,
                            List *param_exprs, bool singlerow, bool binary_mode,
                            uint32 est_entries, Bitmapset *keyparamids) {
    // Create new Memoize node
    Memoize *node = makeNode(Memoize);
    Plan *plan = &node->plan;

    // Copy target list from child plan (no transformation needed)
    plan->targetlist = lefttree->targetlist;
    plan->qual = NIL;  // No additional filtering
    plan->lefttree = lefttree;
    plan->righttree = NULL;  // Memoize is unary operator

    // Set memoization parameters
    node->numKeys = list_length(param_exprs);  // Number of cache key fields
    node->hashOperators = hashoperators;       // Hash operators for keys
    node->collations = collations;             // Collations for key comparison
    node->param_exprs = param_exprs;           // Expressions forming cache key

    // Set optimization flags and estimates
    node->singlerow = singlerow;      // Child produces at most one row
    node->binary_mode = binary_mode;  // Use binary comparison
    node->est_entries = est_entries;  // Expected cache size
    node->keyparamids = keyparamids;  // Parameter IDs in cache key

    return node;
}
```

Key simplifications made:
- Removed detailed comments for clarity
- Focused on the core logic: creating node, setting cache parameters
- Preserved all essential memoization functionality
- Grouped related parameter assignments for better readability