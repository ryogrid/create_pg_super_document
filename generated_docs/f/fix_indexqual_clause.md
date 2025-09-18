# fix_indexqual_clause

## Location
src/backend/optimizer/plan/createplan.c: 5093 - 5163

## Overview
Converts a single indexqual clause to the form needed by PostgreSQL's executor, handling parameter replacement and index key variable transformation.

## Definition


## Detailed Description
This function performs the core transformation logic for individual index qualification clauses, preparing them for execution. It operates in two main phases:

1. **Parameter Replacement**: Uses replace_nestloop_params() to replace any outer-relation variables with nestloop parameters, which also creates a safe copy of the clause for in-place modification.

2. **Index Key Transformation**: Replaces index key variables or expressions with proper index Var nodes that reference the index's attribute numbers rather than the original relation's attribute numbers.

The function handles multiple types of index qualification clauses:
- **OpExpr**: Standard operator expressions (e.g., column = value)
- **RowCompareExpr**: Row comparison expressions for multi-column indexes
- **ScalarArrayOpExpr**: Array comparison expressions (e.g., column = ANY(array))
- **NullTest**: NULL/NOT NULL tests on index columns

For each clause type, it identifies the index key operand(s) and calls fix_indexqual_operand() to perform the actual variable replacement. Row comparisons require special handling to process multiple index columns simultaneously.

## Parameters / Member Variables
- : PlannerInfo structure containing planner context and state
- : IndexOptInfo describing the index being used
- : Index column number being referenced (for single-column cases)
- : The qualification clause to be transformed
- : List of index column numbers (used for multi-column row comparisons)

## Dependencies
- Functions called/Symbols referenced:
  - replace_nestloop_params
  - fix_indexqual_operand
  - forboth (macro)
  - lfirst_int
  - nodeTag
  - IndexOptInfo (struct type)
  - OpExpr (struct type)
  - RowCompareExpr (struct type)
  - ScalarArrayOpExpr (struct type)  
  - NullTest (struct type)
- Called from (representative examples):
  - fix_indexqual_references
  - fix_indexorderby_references

## Notes and Other Information
This function is a critical component in the index scan execution preparation process. It ensures that index qualifications are properly parameterized and use the correct attribute references for the target index. The function creates a copy of the input clause during parameter replacement, making it safe for in-place modifications. The comprehensive handling of different clause types reflects the variety of ways indexes can be used in PostgreSQL queries. Error handling ensures that unsupported qualification types are caught during planning rather than execution. Located in src/backend/optimizer/plan/createplan.c at lines 5093-5163.