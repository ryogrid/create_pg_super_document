# init_dummy_sjinfo

## Location
src/backend/optimizer/path/joinrels.c: 670 - 704

## Overview
Initializes a SpecialJoinInfo structure for a plain inner join between two specified sets of relations, providing minimal required information for join planning functions.

## Definition


## Detailed Description
The  function creates a minimal SpecialJoinInfo structure for inner joins. While inner joins normally don't require SpecialJoinInfo nodes (which are primarily used for outer joins, semijoins, and antijoins), some join planning functions need at least basic information about which relations are being joined.

The function populates the essential fields of the SpecialJoinInfo structure:
- Sets the join type to JOIN_INNER
- Establishes the left and right hand side relation sets
- Initializes commute relationship fields to NULL (no restrictions)
- Sets various join-specific flags to safe default values

This dummy SpecialJoinInfo can be used by cost estimation functions, join relation building, and other planning operations that require a consistent interface regardless of join type.

## Parameters / Member Variables
- : Pointer to the SpecialJoinInfo structure to be initialized
- : Bitmapset identifying the relations on the left side of the join
- : Bitmapset identifying the relations on the right side of the join

## Dependencies
- Functions called/Symbols referenced:
  - T_SpecialJoinInfo (node type)
  - JOIN_INNER (join type constant)
- Called from (representative examples):
  - compute_semi_anti_join_factors
  - approx_tuple_count
  - make_join_rel
  - build_child_join_sjinfo
  - consider_new_or_clause

## Notes and Other Information
- Only populates essential fields needed for basic join planning operations
- Non-essential fields like , ,  are set to safe defaults
- The  field is set to 0 since inner joins don't create outer join relations
- Commute restriction lists are set to NULL, indicating no ordering restrictions
- This function enables consistent handling of all join types through the SpecialJoinInfo interface
- Widely used across different modules including cost estimation, join relation creation, and clause optimization