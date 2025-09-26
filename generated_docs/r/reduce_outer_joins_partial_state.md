# reduce_outer_joins_partial_state

## Location
src/backend/optimizer/prep/prepjointree.c: 84 - 88

## Overview
The reduce_outer_joins_partial_state struct tracks information about full outer joins that have been partially reduced to left or right outer joins, recording which side still requires null-preservation semantics.

## Definition
```c
typedef struct reduce_outer_joins_partial_state
{
    int         full_join_rti;    /* RT index of a formerly-FULL join */
    Relids      unreduced_side;   /* relids in its still-nullable side */
} reduce_outer_joins_partial_state;
```

## Detailed Description
This structure represents a specific case in outer join reduction optimization where a full outer join (FULL JOIN) has been partially optimized. Full outer joins preserve null values from both sides of the join, but when null-rejecting conditions exist for only one side, the full join can be reduced to either a left or right outer join, depending on which side has the null-rejecting conditions.

The structure maintains essential information about such partially reduced joins: the range table index of the original full join and the set of relations on the side that still requires null-preservation. This information is crucial for subsequent optimization phases and for maintaining correct query semantics.

## Parameters / Member Variables
- `full_join_rti`: Range table index of the original full outer join that has been partially reduced
- `unreduced_side`: Set of relation IDs representing the side of the join that still requires null-preservation semantics (i.e., the side that will still produce null values in the result)

## Dependencies
- Functions called/Symbols referenced:
  - Relids (PostgreSQL's bitmap set type for relation IDs)
- Called from (representative examples):
  - reduce_outer_joins
  - report_reduced_full_join

## Notes and Other Information
This structure is specifically used for tracking the intermediate state when full outer joins cannot be completely reduced to inner joins but can be simplified to left or right outer joins. The unreduced_side field is critical for maintaining correct join semantics - it identifies which relations may still produce null values after the partial reduction, ensuring that the optimizer and executor handle null values correctly in subsequent processing.