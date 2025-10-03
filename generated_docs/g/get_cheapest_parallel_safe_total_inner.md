# get_cheapest_parallel_safe_total_inner

## Location
[src/backend/optimizer/path/pathkeys.c:697-737](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/pathkeys.c#L697-L737)

## Overview
Finds the unparameterized parallel-safe path with the least total cost from a list of paths.

## Definition

```c
Path *
get_cheapest_parallel_safe_total_inner(List *paths)
```
## Detailed Description
This function is specifically designed for finding inner paths suitable for parallel join operations. It searches through a list of paths to find the first one that meets two critical criteria: it must be parallel-safe (can be executed safely in a parallel context) and unparameterized (does not depend on outer relation variables). The function assumes that the input paths are already sorted by cost, so it returns the first path that meets the criteria, which will inherently be the cheapest.

This function is commonly used in join planning where the optimizer needs to select an inner path for parallel hash joins, merge joins, or nested loop joins. The parallel-safety requirement ensures the path can be used in parallel query execution, while the unparameterized requirement ensures the inner path can be materialized independently.

## Parameters / Member Variables
- `*paths`: List of possible paths, assumed to be sorted by total cost in ascending order
## Dependencies
- Functions called/Symbols referenced:
  - bms_is_empty
  - PATH_REQ_OUTER
- Called from (representative examples):
  - [add_paths_to_append_rel](../a/add_paths_to_append_rel.md)
  - [sort_inner_and_outer](../s/sort_inner_and_outer.md)
  - [match_unsorted_outer](../m/match_unsorted_outer.md)
  - [hash_inner_and_outer](../h/hash_inner_and_outer.md)

## Notes and Other Information
- Returns the first matching path, assuming paths are pre-sorted by cost
- Returns NULL if no suitable path is found
- Specifically designed for parallel join planning where inner paths must be both parallel-safe and unparameterized
- The function does not perform explicit cost comparison since it relies on pre-sorted input
- Part of PostgreSQL's parallel query execution infrastructure

## Simplified Source

```c
Path *
get_cheapest_parallel_safe_total_inner(List *paths)
{
    ListCell *l;

    // Search for first path that is both parallel-safe and unparameterized
    foreach(l, paths)
    {
        Path *innerpath = (Path *) lfirst(l);

        // Check if path is parallel-safe and has no outer dependencies
        if (innerpath->parallel_safe &&
            bms_is_empty(PATH_REQ_OUTER(innerpath)))
            return innerpath;
    }

    // No suitable path found
    return NULL;
}
```