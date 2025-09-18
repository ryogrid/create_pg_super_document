# get_eclass_for_sort_expr

## Location
src/backend/optimizer/path/equivclass.c: 586 - 758

## Overview
Finds an existing EquivalenceClass containing a given sort expression, or optionally creates a new single-member EquivalenceClass for expressions not yet represented.

## Definition


## Detailed Description
This function is used to integrate sort expressions (from ORDER BY, GROUP BY, etc.) into the EquivalenceClass system. It first searches existing EquivalenceClasses for a match based on expression equality, operator families, and collation. If no match is found and create_it is true, it constructs a new EquivalenceClass.

The function handles several important considerations:

1. **Volatile Expression Handling**: Volatile expressions are only matched when they share the same SortGroupRef, ensuring consistent behavior across references to the same volatile expression.

2. **Child Member Matching**: When rel is specified, child EquivalenceMembers for that specific relation are considered in addition to regular members.

3. **JoinDomain Restrictions**: Constants are only matched within the same JoinDomain to maintain proper scoping.

4. **Expression Validation**: Unlike process_equivalence(), this function must validate that expressions marked as constants don't contain volatile functions, aggregates, set-returning functions, or window functions.

5. **EC Index Maintenance**: When merging is complete, newly created EquivalenceClasses are properly indexed in relation structures for efficient lookup.

## Parameters / Member Variables
- : PlannerInfo containing global optimizer state and existing EquivalenceClasses
- : Sort expression to find or create an EquivalenceClass for
- : List of btree operator families for the expression
- : Input type for the operator class
- : Required collation for the expression
- : SortGroupRef identifier from the originating clause (required for volatile expressions)
- : Specific relation to consider child members for, or NULL to ignore child members
- : Whether to create a new EquivalenceClass if no match is found

## Dependencies
- Functions called/Symbols referenced:
  - canonicalize_ec_expression
  - linitial_node
  - equal
  - bms_equal
  - makeNode
  - list_copy
  - contain_volatile_functions
  - pull_varnos
  - add_eq_member
  - copyObject
  - expression_returns_set
  - contain_agg_clause
  - contain_window_function
  - bms_next_member
  - bms_add_member
- Called from (representative examples):
  - make_pathkey_from_sortinfo
  - convert_subquery_pathkeys
  - initialize_mergeclause_eclasses

## Notes and Other Information
- Safe to use both before and after EquivalenceClass merging since it never causes merging
- Child member matching can be order-dependent when multiple ECs match the same expression
- Volatile EquivalenceClasses require a valid sortref and cannot be matched without one
- New EquivalenceClasses are constructed in the planner memory context for proper lifespan
- Expression canonicalization ensures consistent type and collation exposure
- Returns NULL when no match is found and create_it is false