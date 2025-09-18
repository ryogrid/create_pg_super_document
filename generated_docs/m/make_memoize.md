# make_memoize

## Location
src/backend/optimizer/plan/createplan.c: 6569 - 6593

## Overview
Creates a Memoize plan node that caches the results of its child plan based on parameter values, optimizing performance for repeated executions with the same parameters.

## Definition


## Detailed Description
This function constructs a Memoize plan node, which implements result caching for subplans that are executed multiple times with different parameter values. The Memoize node maintains a hash table where keys are formed from the parameter expressions and values are the cached results. This optimization is particularly effective for parameterized nested loop joins where the inner plan is repeatedly executed with different outer values.

The function initializes all the memoization-specific fields including the hash operators for key comparison, collation information, parameter expressions that form the cache key, and various control flags and estimates.

## Parameters / Member Variables
- : The input Plan node whose results will be cached
- : Array of hash operators for the memoization keys
- : Array of collations for the memoization keys  
- : List of parameter expressions that form the cache key
- : Boolean indicating if the child plan produces at most one row per parameter combination
- : Boolean indicating if binary comparison mode should be used
- : Estimated number of cache entries
- : Bitmapset identifying which parameters are part of the cache key

## Dependencies
- Functions called/Symbols referenced:
  - Memoize (struct type, created with makeNode())
  - Agg (related type)
- Called from (representative examples):
  - create_memoize_plan

## Notes and Other Information
- This is a static function, accessible only within the same source file
- The Memoize node has no right child (righttree is set to NULL)  
- No additional qualification conditions are applied (qual is set to NIL)
- The target list is directly copied from the child plan
- The numKeys field is automatically calculated from the length of param_exprs
- Memoization is most effective when the same parameter values are likely to be repeated
- Located in src/backend/optimizer/plan/createplan.c at lines 6569-6593