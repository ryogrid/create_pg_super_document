# make_outerjoininfo

## Location
src/backend/optimizer/plan/initsplan.c: 1360 - 1699

## Overview
Builds a SpecialJoinInfo structure for the current outer join, determining ordering constraints and commutability relationships with other joins in the query tree.

## Definition


## Detailed Description
The  function creates and initializes a SpecialJoinInfo structure that captures essential metadata about an outer join operation. This function is critical for the PostgreSQL optimizer's ability to understand join ordering constraints and determine which joins can be safely reordered or commuted.

The function performs several key tasks:
1. Validates join types and enforces restrictions (e.g., FOR UPDATE clauses cannot be applied to nullable sides)
2. Computes minimum left-hand and right-hand relation sets required for the join
3. Analyzes relationships with previously processed outer joins to determine commutability
4. Handles PlaceHolderVar constraints that affect join ordering
5. Applies outer join identity rules to optimize join reordering where possible

The function assumes bottom-up processing, meaning all syntactically lower outer joins have already been processed and are available in root->join_info_list.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning state and optimizer information
- : Bitmap of base+OJ relation IDs syntactically on the outer (left) side of the join
- : Bitmap of base+OJ relation IDs syntactically on the inner (right) side of the join  
- : Bitmap of base+OJ relation IDs participating in inner joins below this outer join
- : Type of join operation (must be LEFT, FULL, SEMI, or ANTI)
- : Range table index of the join RTE (0 for SEMI joins which aren't in the RT list)
- : Join condition for the outer join in implicit-AND format

## Dependencies
- Functions called/Symbols referenced:
  - compute_semijoin_info
  - pull_varnos
  - find_nonnullable_rels
  - contain_placeholder_references_to
  - bms_* (various bitmap set operations)
  - LCS_asString
- Called from (representative examples):
  - deconstruct_distribute

## Notes and Other Information
- The function enforces that FOR UPDATE/SHARE cannot be applied to nullable sides of outer joins, as the executor doesn't support this
- Full joins are treated as optimization barriers - the optimizer cannot associate into or out of them
- The function implements outer join identity rules, particularly identity 3, which allows certain join commutations when strictness conditions are met
- PlaceHolderVar handling ensures that expressions are evaluated at appropriate join levels
- Commutability relationships are tracked bidirectionally between SpecialJoinInfo structures
- The returned SpecialJoinInfo should be appended to root->join_info_list by the caller
- Empty min_lefthand or min_righthand sets are expanded to their full respective sides to avoid confusion in later processing