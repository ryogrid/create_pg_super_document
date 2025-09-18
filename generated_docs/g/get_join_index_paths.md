# get_join_index_paths

## Location
src/backend/optimizer/path/indxpath.c: 600 - 677

## Overview
Generates index paths using clauses from specified outer relations by collecting applicable join clauses, restriction clauses, and delegating to get_index_paths for actual path creation.

## Definition


## Detailed Description
This function serves as the final step in parameterized index path generation, responsible for:

1. **Duplicate avoidance**: Checks if the given relids set has already been processed and returns early if so
2. **Clause collection**: Systematically gathers applicable clauses for each index column:
   - **Simple join clauses**: From jclauseset, includes all clauses whose clause_relids are a subset of the target relids
   - **EquivalenceClass clauses**: From eclauseset, includes at most one clause per column (since they're redundant)
   - **Restriction clauses**: From rclauseset, adds all restriction clauses to supplement the join clauses
3. **Path generation**: Calls get_index_paths() with the collected clause set to create actual index paths
4. **Tracking**: Records the processed relids set in considered_relids to prevent future duplicate work

The function implements a special rule for EquivalenceClass clauses: since clauses generated for each column are redundant, it uses only the first applicable clause per column, breaking after finding one.

## Parameters / Member Variables
- : PlannerInfo containing query planning context
- : RelOptInfo for the index's heap relation  
- : IndexOptInfo for the index to generate paths for
- : IndexClauseSet containing indexable restriction clauses
- : IndexClauseSet containing indexable simple join clauses
- : IndexClauseSet containing indexable EquivalenceClass clauses
- : Output list for bitmap index paths
- : Current set of relation IDs to consider (target rel plus outer rels)
- : Input/output list tracking processed relation sets

## Dependencies
- Functions called/Symbols referenced:
  - list_member
  - MemSet
  - bms_is_subset
  - list_concat
  - get_index_paths
- Called from (representative examples):
  - consider_index_join_outer_rels

## Notes and Other Information
- Acts as the workhorse for consider_index_join_clauses, implementing the core clause collection logic
- The assertion Assert(clauseset.nonempty) ensures that the caller provided meaningful relids (should always find applicable clauses)
- Restriction clauses are always included since they don't depend on outer relations and can only improve selectivity
- The special handling of EquivalenceClass clauses prevents redundant clause application while maintaining correctness
- Records processed relids in considered_relids to enable efficient duplicate detection across the entire planning process
- Delegates actual path creation to get_index_paths(), maintaining separation of concerns between clause collection and path generation