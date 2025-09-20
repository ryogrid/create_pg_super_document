# choose_bitmap_and

## Location
[src/backend/optimizer/path/indxpath.c:1287-1492](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L1287-L1492)

## Overview
 takes a list of bitmap paths and intelligently combines them into a single path, choosing an optimal subset to balance selectivity and computational cost.

## Definition

```c
static Path *
choose_bitmap_and(PlannerInfo *root, RelOptInfo *rel, List *paths)
```
## Detailed Description
This sophisticated optimization function implements a multi-stage heuristic algorithm to select the best combination of bitmap index paths for AND operations:

**Stage 1 - Deduplication**: Groups paths that use identical sets of WHERE clauses and index predicates, keeping only the cheapest-to-scan path in each group. This eliminates redundant indexes that include the same interesting columns plus irrelevant ones.

**Stage 2 - Sorting**: Sorts remaining paths by index access cost with cheapest first.

**Stage 3 - Combination Selection**: Uses an O(N²) algorithm rather than exhaustive O(2^N) enumeration:
- For each path as a potential "AND group leader"
- Progressively considers adding subsequent (higher-cost) paths
- Keeps additions only if they reduce the estimated total scan cost
- Maintains the cheapest combination found

**Redundancy Prevention**: Implements strict redundancy checks to avoid double-counting selectivity:
- **Clause Redundancy**: Rejects combinations where indexes use the same WHERE clauses
- **Predicate Redundancy**: Rejects partial indexes whose predicate clauses are implied by already-selected conditions

The function handles edge cases efficiently, returning single paths unchanged and creating  objects only when multiple paths are beneficial.

## Parameters / Member Variables
- : PlannerInfo containing planner state and configuration
- : RelOptInfo representing the relation being scanned
- : List of candidate bitmap paths to potentially combine

## Dependencies
- Functions called/Symbols referenced:
  - [classify_index_clause_usage](classify_index_clause_usage.md)
  - [cost_bitmap_tree_node](cost_bitmap_tree_node.md)
  - [bitmap_scan_cost_est](../b/bitmap_scan_cost_est.md)
  - [bitmap_and_cost_est](../b/bitmap_and_cost_est.md)
  - [create_bitmap_and_path](create_bitmap_and_path.md)
  - [predicate_implied_by](../p/predicate_implied_by.md)
  - [path_usage_comparator](../p/path_usage_comparator.md)
- Called from (representative examples):
  - [generate_bitmap_or_paths](../g/generate_bitmap_or_paths.md)
  - [create_index_paths](create_index_paths.md)

## Notes and Other Information
- Returns either a single input path or a new BitmapAndPath combining multiple inputs
- The O(N²) complexity makes it practical even with many candidate paths after deduplication
- Redundancy detection prevents costsize.c and clausesel.c from double-counting clauses
- Handles both regular WHERE clause redundancy and index predicate clause implications
- The prefiltering stage can dramatically reduce path count in systems with many variant indexes
- Uses  structures to efficiently track clause usage patterns