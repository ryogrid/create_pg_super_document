# extract_nonindex_conditions

## Location
src/backend/optimizer/path/costsize.c: 840 - 897

## Overview
Extracts qualification clauses that cannot be handled by the index machinery and must be applied as qpquals during index scanning.

## Definition
```c
static List *extract_nonindex_conditions(List *qual_clauses, List *indexclauses)
```

## Detailed Description
The `extract_nonindex_conditions` function separates qualification clauses into those that can be handled by the index access method versus those that must be evaluated as additional qualifications (qpquals) after tuple retrieval. This separation is crucial for accurate cost estimation since qpquals require CPU evaluation costs that are not covered by index access costs.

The function filters out pseudoconstant conditions (which can be dropped) and conditions that are redundant with existing index clauses (either direct duplicates or clauses derived from the same EquivalenceClass). The logic is designed to match the behavior of create_indexscan_plan() but performs only basic redundancy detection for performance reasons, leaving more sophisticated redundancy elimination to the plan creation phase.

## Parameters / Member Variables
- `qual_clauses`: List of RestrictInfo nodes representing qualification clauses to be evaluated
- `indexclauses`: List of IndexClause nodes representing conditions that the index can handle directly

## Dependencies
- Functions called/Symbols referenced:
  - is_redundant_with_indexclauses
  - NIL (constant)
  - lfirst_node (macro)
  - lappend (function)
  - RestrictInfo (structure)
- Called from (representative examples):
  - cost_index
  - cost_qual_eval_context

## Notes and Other Information
This function performs only basic redundancy checking for efficiency reasons, as it's called during cost estimation where performance is critical. The more comprehensive redundancy elimination happens later in create_indexscan_plan() during actual plan construction. The function is essential for separating index-level filtering from tuple-level filtering, which have different cost characteristics in PostgreSQL's cost model.