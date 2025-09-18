# extract_lateral_references

## Location
[src/backend/optimizer/plan/initsplan.c:406-500](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/initsplan.c#L406-L500)

## Overview
Extracts variable references from LATERAL relations and processes them for use in join planning.

## Definition


## Detailed Description
This function is responsible for identifying and processing variable references within LATERAL relations during query planning. LATERAL relations can reference columns from relations that appear earlier in the FROM clause, creating dependencies that need to be tracked for proper join ordering. The function extracts these lateral references, adjusts their variable levels, and makes them available for subsequent join planning operations.

The function handles different types of range table entries (RTEs) including relations with table samples, subqueries, functions, table functions, and VALUES clauses. For each type, it uses the appropriate mechanism to extract variables at the correct nesting level, then processes these variables to ensure they reference the correct scope.

## Parameters / Member Variables
- : The PlannerInfo structure containing global planning information
- : The RelOptInfo structure for the base relation being processed
- : The range table index of the relation being analyzed

## Dependencies
- Functions called/Symbols referenced:
  - [pull_vars_of_level](../p/pull_vars_of_level.md)
  - copyObject
  - [IncrementVarSublevelsUp](../I/IncrementVarSublevelsUp.md)
  - [preprocess_phv_expression](../p/preprocess_phv_expression.md)
  - [list_free](../l/list_free.md)
  - [bms_make_singleton](../b/bms_make_singleton.md)
  - [add_vars_to_targetlist](../a/add_vars_to_targetlist.md)
- Called from (representative examples):
  - [find_lateral_references](../f/find_lateral_references.md)

## Notes and Other Information
- Only processes relations marked as LATERAL (rte->lateral == true)
- Handles different RTE types: RTE_RELATION, RTE_SUBQUERY, RTE_FUNCTION, RTE_TABLEFUNC, RTE_VALUES
- Adjusts variable levels to match the current query level (varlevelsup = 0)
- Special handling for PlaceHolderVars including expression preprocessing for subquery-derived PHVs
- Stores the processed lateral variables in brel->lateral_vars for later use by create_lateral_join_info
- Uses a simplified approach of marking all variables as needed at the LATERAL RTE rather than computing separate dependencies for each variable