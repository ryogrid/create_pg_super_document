# add_partial_path

## Location
[src/backend/optimizer/util/pathnode.c:747-864](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L747-L864)

## Overview
Manages partial paths for parallel query execution by maintaining an ordered list of viable partial execution paths for a relation, considering only pathkeys and total cost while ensuring parallel safety.

## Definition

```c
void
add_partial_path(RelOptInfo *parent_rel, Path *new_path)
```
## Detailed Description
The  function is responsible for maintaining the  of a relation by adding new partial paths while removing dominated ones. Unlike regular paths, partial paths are designed for parallel execution where multiple workers can execute portions of the path simultaneously, each generating a subset of the overall result.

Key characteristics:
- Maintains partial_pathlist sorted by total cost (cheapest first)
- Only considers pathkeys and total cost (not parameterization or startup costs)
- Ensures all paths are parallel-safe
- Does not handle parameterized partial paths due to safety concerns with parallel execution
- Uses fuzzy cost comparison (STD_FUZZ_FACTOR) to avoid removing paths with very similar costs

The function implements a dominance-based pruning algorithm similar to , but simplified since partial paths don't need to consider parameterization, startup costs, or row count differences.

## Parameters / Member Variables
- `*parent_rel`: The RelOptInfo structure representing the relation to which the partial path will be added
- `*new_path`: The new partial Path to be considered for addition to the partial_pathlist
## Dependencies
- Functions called/Symbols referenced:
  - [compare_pathkeys](../c/compare_pathkeys.md)
  - foreach_delete_current
  - foreach_current_index
  - [list_insert_nth](../l/list_insert_nth.md)
  - [PathKeysComparison](../P/PathKeysComparison.md) (enum)
  - PATHKEYS_DIFFERENT, PATHKEYS_BETTER1, PATHKEYS_BETTER2 (constants)
  - STD_FUZZ_FACTOR (constant)

- Called from (representative examples):
  - [create_plain_partial_paths](../c/create_plain_partial_paths.md)
  - [build_index_paths](../b/build_index_paths.md)
  - [try_partial_nestloop_path](../t/try_partial_nestloop_path.md)
  - [try_partial_mergejoin_path](../t/try_partial_mergejoin_path.md)
  - [try_partial_hashjoin_path](../t/try_partial_hashjoin_path.md)
  - [create_partial_grouping_paths](../c/create_partial_grouping_paths.md)

## Notes and Other Information
- The function assumes that GatherPaths are not created until all partial paths for a relation are complete
- Unlike add_path, no special exception is made for IndexPaths since partial index paths won't be referenced by partial BitmapHeapPaths
- The function uses CHECK_FOR_INTERRUPTS() to allow query cancellation during potentially long operations
- Paths determined to be dominated are immediately freed with pfree() to prevent memory leaks
- The parallel safety requirement is enforced through assertions on both the new path and the parent relation

## Simplified Source

```c
void
add_partial_path(RelOptInfo *parent_rel, Path *new_path)
{
    bool accept_new = true;
    int insert_at = 0;
    ListCell *p1;

    // Safety checks for parallel execution
    CHECK_FOR_INTERRUPTS();
    Assert(new_path->parallel_safe);
    Assert(parent_rel->consider_parallel);

    // Compare new path against all existing partial paths
    foreach(p1, parent_rel->partial_pathlist)
    {
        Path *old_path = (Path *) lfirst(p1);
        bool remove_old = false;

        // Compare pathkeys to determine path quality
        PathKeysComparison keyscmp = compare_pathkeys(new_path->pathkeys, old_path->pathkeys);

        if (keyscmp != PATHKEYS_DIFFERENT)
        {
            // Decide which path to keep based on cost and pathkeys
            if (new_path->total_cost > old_path->total_cost * STD_FUZZ_FACTOR)
            {
                // New path is significantly more expensive
                if (keyscmp != PATHKEYS_BETTER1)
                    accept_new = false;
            }
            else if (old_path->total_cost > new_path->total_cost * STD_FUZZ_FACTOR)
            {
                // Old path is significantly more expensive
                if (keyscmp != PATHKEYS_BETTER2)
                    remove_old = true;
            }
            else if (keyscmp == PATHKEYS_BETTER1)
            {
                // Similar costs, new path has better ordering
                remove_old = true;
            }
            else if (keyscmp == PATHKEYS_BETTER2)
            {
                // Similar costs, old path has better ordering
                accept_new = false;
            }
            else if (old_path->total_cost > new_path->total_cost * 1.0000000001)
            {
                // Same pathkeys, old path slightly more expensive
                remove_old = true;
            }
            else
            {
                // Essentially equivalent paths, keep the old one
                accept_new = false;
            }
        }

        // Remove dominated old path or track insertion position
        if (remove_old)
        {
            parent_rel->partial_pathlist = foreach_delete_current(parent_rel->partial_pathlist, p1);
            pfree(old_path);
        }
        else
        {
            // Maintain cost-ordered list
            if (new_path->total_cost >= old_path->total_cost)
                insert_at = foreach_current_index(p1) + 1;
        }

        if (!accept_new)
            break;
    }

    // Add new path or free it if rejected
    if (accept_new)
        parent_rel->partial_pathlist = list_insert_nth(parent_rel->partial_pathlist, insert_at, new_path);
    else
        pfree(new_path);
}
```