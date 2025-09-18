# consider_index_join_outer_rels

## Location
src/backend/optimizer/path/indxpath.c: 497 - 599

## Overview
Generates parameterized index paths by systematically examining combinations of outer relation sets from join clauses, implementing the core logic for parameterized path enumeration with heuristic limits to prevent exponential explosion.

## Definition


## Detailed Description
This function serves as the workhorse for consider_index_join_clauses, implementing the detailed logic for generating parameterized index paths. For each join clause in the input list, it:

1. **Extracts relation sets**: Gets the clause_relids from each IndexClause to understand which relations are involved
2. **Avoids redundancy**: Skips relation sets already processed (tracked in considered_relids)
3. **Generates combinations**: Creates union sets by combining the current clause's relids with each previously-tried set, ensuring exploration of useful clause combinations
4. **Applies heuristic limits**: Caps the number of relation sets at 10 * considered_clauses to prevent exponential growth in planning time
5. **Handles EquivalenceClasses**: Uses eclass_already_used() to avoid redundant combinations when clauses derive from the same EquivalenceClass
6. **Delegates path creation**: Calls get_join_index_paths() for each viable relation set to actually generate the paths

The function implements both combination logic (trying clauses together) and individual processing (trying each clause alone).

## Parameters / Member Variables
- : PlannerInfo containing query planning context
- : RelOptInfo for the index's heap relation
- : IndexOptInfo for the index to generate paths for
- : IndexClauseSet containing indexable restriction clauses
- : IndexClauseSet containing indexable simple join clauses  
- : IndexClauseSet containing indexable EquivalenceClass clauses
- : Output list for bitmap index paths
- : List of IndexClauses for join clauses to process
- : Total count of clauses considered (for heuristic limit)
- : Input/output list tracking all relation sets already processed

## Dependencies
- Functions called/Symbols referenced:
  - bms_subset_compare
  - eclass_already_used
  - get_join_index_paths
  - bms_union
  - list_member
  - list_nth
- Called from (representative examples):
  - consider_index_join_clauses

## Notes and Other Information
- Uses BMS_DIFFERENT check to avoid subset relationships that wouldn't generate new information
- Implements a 10 * considered_clauses heuristic limit to prevent exponential planning time growth
- Carefully avoids revisiting newly-added entries in considered_relids during the same loop iteration
- Handles both EquivalenceClass-derived clauses and regular join clauses uniformly
- The subset check (bms_subset_compare) is a quick redundancy filter; get_join_index_paths performs more thorough duplicate detection
- Always tries each clause's relation set individually, even when combination limits are exceeded