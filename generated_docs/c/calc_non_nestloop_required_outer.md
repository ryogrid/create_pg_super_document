# calc_non_nestloop_required_outer

## Location
[src/backend/optimizer/util/pathnode.c:2405-2456](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L2405-L2456)

## Overview
Computes the required outer relation set for merge join and hash join paths by combining parameter requirements from both input paths.

## Definition
```c
Relids calc_non_nestloop_required_outer(Path *outer_path, Path *inner_path)
```

## Detailed Description
This function calculates the set of relations that must be provided as parameters from outside a merge or hash join. Unlike nested loop joins, these join types cannot pass parameters between their left and right sides during execution, so both input paths must receive all their required parameters from outside the join. The function validates that neither input path depends on relations from the other side, then forms the union of their parameter requirements. It properly handles relation ID mapping for partitioned tables by considering top-level parent relations.

## Parameters / Member Variables
- `outer_path`: Path representing the outer (left) side of the join
- `inner_path`: Path representing the inner (right) side of the join

## Dependencies
- Functions called/Symbols referenced:
  - PATH_REQ_OUTER (macro to extract required outer relations from a path)
  - [bms_overlap](../b/bms_overlap.md) (to validate no cross-dependencies between input paths)
  - [bms_union](../b/bms_union.md) (to combine parameter requirements from both paths)
  - Assert (for debugging constraint validation)
  - PG_USED_FOR_ASSERTS_ONLY (annotation for debug-only variables)
- Called from (representative examples):
  - [try_mergejoin_path](../t/try_mergejoin_path.md) (in joinpath.c)
  - [try_hashjoin_path](../t/try_hashjoin_path.md) (in joinpath.c)

## Notes and Other Information
- This function is used for both merge joins and hash joins, which have similar parameter passing constraints
- Unlike nested loop joins, merge and hash joins cannot pass parameters between their input sides during execution
- The function uses top_parent_relids when available to handle partitioned table scenarios correctly
- Validation ensures that neither input path requires relations that will be provided by the other input
- The bms_union function correctly handles empty parameter sets, so no explicit empty check is needed
- Memory management: the result does not share storage with the input paths to prevent corruption
- This is a core utility function for non-nested loop join planning in the PostgreSQL query optimizer

## Simplified Source

```c
Relids calc_non_nestloop_required_outer(Path *outer_path, Path *inner_path) {
    // Get parameter requirements from both input paths
    Relids outer_paramrels = PATH_REQ_OUTER(outer_path);
    Relids inner_paramrels = PATH_REQ_OUTER(inner_path);

    // Get relation IDs, using top-parent for partitioned tables
    Relids outer_relids = outer_path->parent->top_parent_relids ?
                         outer_path->parent->top_parent_relids :
                         outer_path->parent->relids;

    Relids inner_relids = inner_path->parent->top_parent_relids ?
                         inner_path->parent->top_parent_relids :
                         inner_path->parent->relids;

    // Validate: neither path can depend on relations from the other side
    // (merge/hash joins cannot pass parameters between input sides)
    Assert(!bms_overlap(outer_paramrels, inner_relids));
    Assert(!bms_overlap(inner_paramrels, outer_relids));

    // Combine parameter requirements from both sides
    return bms_union(outer_paramrels, inner_paramrels);
}
```