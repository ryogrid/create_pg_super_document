# reduce_outer_joins_pass1_state

## Location
[src/backend/optimizer/prep/prepjointree.c:71-76](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L71-L76)

## Overview
The reduce_outer_joins_pass1_state struct maintains state information during the first pass of outer join reduction optimization, tracking relation sets and outer join presence within query subtrees to enable intelligent join simplification.

## Definition
```c
typedef struct reduce_outer_joins_pass1_state
{
    Relids      relids;         /* base relids within this subtree */
    bool        contains_outer; /* does subtree contain outer join(s)? */
    List       *sub_states;     /* List of states for subtree components */
} reduce_outer_joins_pass1_state;
```

## Detailed Description
This structure is used during the first phase of PostgreSQL's outer join reduction optimization process. The outer join reduction optimization attempts to convert outer joins to inner joins when the query conditions make the outer join semantics unnecessary, potentially improving query performance significantly.

The first pass analyzes the join tree structure to collect information about which base relations are present in each subtree and whether any outer joins exist. This information is crucial for the second pass, which makes the actual decisions about whether specific outer joins can be safely reduced to inner joins based on the presence of null-rejecting conditions.

## Parameters / Member Variables
- `relids`: Set of base relation IDs contained within this subtree of the join tree
- `contains_outer`: Boolean flag indicating whether this subtree contains any outer join operations
- `sub_states`: List of reduce_outer_joins_pass1_state structures for child subtrees, enabling recursive processing of the join tree

## Dependencies
- Functions called/Symbols referenced:
  - Relids (PostgreSQL's bitmap set type for relation IDs)
  - List (PostgreSQL's list data structure)
- Called from (representative examples):
  - reduce_outer_joins
  - reduce_outer_joins_pass1
  - reduce_outer_joins_pass2

## Notes and Other Information
This structure is part of a two-pass algorithm for outer join reduction. The first pass (using this structure) is a bottom-up traversal that collects information about the join tree structure. The second pass uses this collected information to make optimization decisions. The sub_states list enables the algorithm to maintain hierarchical state information that mirrors the structure of the join tree itself.