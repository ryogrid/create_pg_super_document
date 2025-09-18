# fix_indexqual_references

## Location
src/backend/optimizer/plan/createplan.c: 5023 - 5063

## Overview
Adjusts indexqual clauses to the form required by PostgreSQL's executor's indexqual machinery, handling qual extraction, parameter replacement, and index key mapping.

## Definition


## Detailed Description
This function performs three critical transformations on index qualification clauses to prepare them for execution:

1. **Qual Clause Selection and Cleanup**: Extracts actual qualification clauses from the input IndexClause list and removes RestrictInfo wrapper nodes, producing clean qualification expressions.

2. **Parameter Replacement**: Replaces outer-relation Var or PlaceHolderVar nodes with nestloop Params to enable proper parameter passing in nested loop joins involving index scans.

3. **Index Key Mapping**: Ensures index keys are represented by Var nodes with varattno set to the index's attribute number rather than the original relation's attribute number, which is essential for correct index access.

The function processes each IndexClause in the index path, iterating through the indexquals within each clause. For each qualification, it creates both a stripped version (with RestrictInfo removed) and a fixed version (with all transformations applied). The fixed version is a complete copy that shares no substructure with the original, which is necessary when subplans are present to avoid execution conflicts.

## Parameters / Member Variables
- : PlannerInfo structure containing planner context and state
- : IndexPath representing the index scan path being processed
- : Output parameter receiving list of qual clauses with RestrictInfo removed
- : Output parameter receiving list of adjusted quals ready for execution

## Dependencies
- Functions called/Symbols referenced:
  - fix_indexqual_clause
  - IndexPath (struct type)
  - IndexOptInfo (struct type)  
  - IndexClause (struct type)
- Called from (representative examples):
  - create_indexscan_plan

## Notes and Other Information
This function is a key component in the index scan plan creation process, bridging the gap between the optimizer's representation of index qualifications and the executor's requirements. The comment indicates that parameter replacement responsibility may be moved elsewhere in future versions. The function ensures that both the original and transformed versions of qualifications are available, which is important for various execution scenarios. The complete copying of substructure prevents issues with shared subplan trees during execution. Located in src/backend/optimizer/plan/createplan.c at lines 5023-5063.