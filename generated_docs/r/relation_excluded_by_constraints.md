# relation_excluded_by_constraints

## Location
src/backend/optimizer/util/plancat.c: 1576 - 1763

## Overview
Determines whether a relation can be excluded from scanning based on constraint analysis, detecting self-inconsistent restrictions or restrictions that contradict the relation's constraints.

## Definition


## Detailed Description
This function performs constraint exclusion optimization by analyzing whether a relation needs to be scanned at all. It examines the relation's base restriction clauses and compares them against the relation's constraints (CHECK constraints, NOT NULL constraints, and partition constraints) to determine if the restrictions are logically inconsistent with the constraints, making the scan unnecessary.

The function operates in multiple phases:
1. **Constant FALSE detection**: Identifies restriction clauses that are constant FALSE or NULL
2. **Constraint exclusion mode checking**: Respects the  GUC setting (off/partition/on)
3. **Self-contradiction analysis**: Checks if restriction clauses contradict each other
4. **Constraint contradiction analysis**: Verifies if restrictions are refuted by relation constraints

The function only works with simple relations and requires immutable functions to make safe deductions during planning.

## Parameters / Member Variables
- : PlannerInfo containing global planner state and configuration
- : RelOptInfo representing the relation being analyzed (must be a simple relation)
- : RangeTblEntry containing metadata about the relation from the range table

## Dependencies
- Functions called/Symbols referenced:
  - IS_SIMPLE_REL (macro for checking simple relation type)
  - contain_mutable_functions (checks for mutable functions in expressions)
  - predicate_refuted_by (logical refutation testing)
  - get_relation_constraints (retrieves relation's constraint expressions)
  - CONSTRAINT_EXCLUSION_OFF/PARTITION/ON (GUC setting values)
  - RELOPT_BASEREL, RELOPT_OTHER_MEMBER_REL (relation optimization kinds)
  - RTE_RELATION (range table entry kind)

- Called from (representative examples):
  - set_rel_size (src/backend/optimizer/path/allpaths.c:364)
  - set_append_rel_size (src/backend/optimizer/path/allpaths.c:1027)

## Notes and Other Information
- Only processes simple relations (Assert(IS_SIMPLE_REL(rel)) enforced)
- Respects the constraint_exclusion GUC setting to control optimization behavior
- Uses weak refutation for self-contradictory restrictions, strong refutation for constraint contradictions
- Handles inheritance scenarios carefully, considering NO INHERIT and NOT NULL constraint inheritance
- Critical for partition pruning and constraint-based optimization in PostgreSQL query planning
- Location: src/backend/optimizer/util/plancat.c:1576-1763