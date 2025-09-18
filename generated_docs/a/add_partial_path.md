# add_partial_path

## Location
[src/backend/optimizer/util/pathnode.c:747-864](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L747-L864)

## Overview
Manages partial paths for parallel query execution by maintaining an ordered list of viable partial execution paths for a relation, considering only pathkeys and total cost while ensuring parallel safety.

## Definition


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
- : The RelOptInfo structure representing the relation to which the partial path will be added
- : The new partial Path to be considered for addition to the partial_pathlist

## Dependencies
- Functions called/Symbols referenced:
  - [compare_pathkeys](../c/compare_pathkeys.md)
  - foreach_delete_current
  - foreach_current_index
  - [list_insert_nth](../l/list_insert_nth.md)
  - PathKeysComparison (enum)
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