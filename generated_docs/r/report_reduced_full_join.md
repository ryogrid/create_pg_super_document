# report_reduced_full_join

## Location
src/backend/optimizer/prep/prepjointree.c: 3358 - 3426

## Overview
Helper function that records information about partially reduced FULL JOINs for later processing in the outer join reduction optimization.

## Definition
```c
static void report_reduced_full_join(reduce_outer_joins_pass2_state *state2,
                                    int rtindex, Relids relids)
```

## Detailed Description
This function creates and stores a record of a FULL JOIN that has been partially reduced (converted to either LEFT JOIN or RIGHT JOIN) during the outer join reduction optimization process. When a FULL JOIN can only be reduced on one side due to nullability constraints, this function captures the necessary information for subsequent cleanup operations.

The function allocates and initializes a reduce_outer_joins_partial_state structure containing:
- The range table index of the FULL JOIN that was reduced
- The relation IDs of the side that remains unreduced (still potentially nullable)

This information is essential for the later phase of outer join reduction where nulling relation markers need to be removed. Since partially reduced FULL JOINs require custom processing (unlike fully reduced joins that can be handled in batch), each one needs its own state record with specific details about which side remains nullable.

The function is called specifically when:
1. A FULL JOIN is reduced to LEFT JOIN (right side has nullability constraints, left side recorded as unreduced)
2. A FULL JOIN is reduced to RIGHT JOIN (left side has nullability constraints, right side recorded as unreduced)

## Parameters / Member Variables
- `state2`: Pass 2 state structure where partial reduction information is accumulated
- `rtindex`: Range table index of the FULL JOIN being partially reduced
- `relids`: Bitmapset of relation IDs for the side that remains unreduced (nullable)

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - lappend
- Called from (representative examples):
  - [reduce_outer_joins_pass2](reduce_outer_joins_pass2.md) (when FULL JOIN is reduced to LEFT or RIGHT JOIN)

## Notes and Other Information
- Static helper function internal to prepjointree.c
- Part of the outer join reduction optimization infrastructure
- The created state records are later processed individually to remove appropriate nulling relation references
- Each partially reduced FULL JOIN requires separate processing because the unreduced side differs for each case
- Memory allocation uses palloc, consistent with PostgreSQLs memory management patterns
- The function is only called during the specific case of partial FULL JOIN reduction, not for complete reductions to INNER JOIN