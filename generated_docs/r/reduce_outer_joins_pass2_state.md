# reduce_outer_joins_pass2_state

## Location
[src/backend/optimizer/prep/prepjointree.c:78-82](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L78-L82)

## Overview
The reduce_outer_joins_pass2_state struct tracks the results of outer join reduction during the second pass, recording which outer joins have been successfully converted to inner joins and which full joins have been partially reduced.

## Definition
```c
typedef struct reduce_outer_joins_pass2_state
{
    Relids      inner_reduced;    /* OJ relids reduced to plain inner joins */
    List       *partial_reduced;  /* List of partially reduced FULL joins */
} reduce_outer_joins_pass2_state;
```

## Detailed Description
This structure serves as the result collector for the second pass of PostgreSQL's outer join reduction optimization. While the first pass analyzes the join tree structure, the second pass performs the actual optimization work by identifying outer joins that can be safely converted to inner joins based on null-rejecting conditions in the query.

The structure distinguishes between fully reduced outer joins (converted to inner joins) and partially reduced full joins. Full outer joins can sometimes be partially reduced to left or right outer joins when only one side has null-rejecting conditions. This fine-grained tracking enables the optimizer to apply the maximum possible simplification to the join tree.

## Parameters / Member Variables
- `inner_reduced`: Set of outer join relation IDs that have been successfully reduced to plain inner joins due to the presence of null-rejecting conditions
- `partial_reduced`: List of full outer joins that have been partially reduced (e.g., converted from FULL JOIN to LEFT JOIN or RIGHT JOIN) but could not be fully converted to inner joins

## Dependencies
- Functions called/Symbols referenced:
  - Relids (PostgreSQL's bitmap set type for relation IDs)
  - [List](../L/List.md) (PostgreSQL's list data structure)
- Called from (representative examples):
  - [reduce_outer_joins](reduce_outer_joins.md)
  - [reduce_outer_joins_pass2](reduce_outer_joins_pass2.md)
  - [report_reduced_full_join](report_reduced_full_join.md)

## Notes and Other Information
This structure is the complement to reduce_outer_joins_pass1_state, working together in a two-pass optimization algorithm. The tracking of partial reductions is particularly important for full outer joins, which may have asymmetric null-rejecting conditions that allow reduction to left or right outer joins but not complete elimination. The information collected here is used to update the query tree structure with the optimized join types.