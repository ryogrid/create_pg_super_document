# deconstruct_distribute_oj_quals

## Location
src/backend/optimizer/plan/initsplan.c: 1878 - 2118

## Overview
Adjusts LEFT JOIN qualifiers for commuted-left-join cases and distributes them into the appropriate joinqual lists and EquivalenceClass structures.

## Definition


## Detailed Description
The  function handles the complex task of processing postponed outer join qualifiers after the main deconstruct_distribute scan is complete. This function is critical for implementing outer join identity optimizations, particularly identity 3, which allows certain LEFT JOINs to commute under specific conditions.

The function performs several key operations:
1. Recomputes syntactic and semantic scopes for the current left join
2. Determines if the join can commute with other joins based on outer join identity rules
3. Generates multiple variants of join clauses with different nullingrels labeling when commutation is possible
4. Distributes the processed qualifiers to appropriate relation structures and creates EquivalenceClasses

When commutation is possible, the function creates different versions of the join conditions corresponding to various join orderings that are semantically equivalent. This enables the optimizer to consider more execution plans while maintaining correctness of NULL-generation semantics.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning state and optimizer information
- : Complete list of JoinTreeItems in depth-first order from the deconstruct scan
- : Specific JoinTreeItem containing postponed oj_joinclauses that need processing

## Dependencies
- Functions called/Symbols referenced:
  - bms_union, bms_add_member, bms_del_member, bms_make_singleton (bitmap operations)
  - remove_nulling_relids
  - add_nulling_relids
  - distribute_quals_to_rels
  - bms_copy
  - bms_equal
  - bms_is_member
  - bms_is_empty
- Called from (representative examples):
  - deconstruct_jointree

## Notes and Other Information
- The function only processes joins where lhs_strict is true, as indicated by the assertion
- When generating qual variants for commuting joins, it processes them in syntactic nesting order using the jtitems list
- EquivalenceClasses are generated only from the first form of quals (with fewest nullingrels bits) to avoid creating nonsensical equivalences
- The function implements proper nullingrels bit manipulation to maintain correct NULL semantics when joins are reordered
- Serial number management ensures that RestrictInfos for the "same" qual condition get identical serial numbers for duplicate detection
- The incompatible_joins mechanism prevents quals from being applied at incorrect join levels
- When no commutation is possible, the function simply distributes the postponed clauses as-is without creating variants