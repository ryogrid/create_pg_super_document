# fix_indexqual_operand

## Location
src/backend/optimizer/plan/createplan.c: 5164 - 5238

## Overview
Converts an indexqual expression to a Var node referencing the index column, representing index keys with varno == INDEX_VAR and varattno equal to the index's attribute number.

## Definition


## Detailed Description
The function transforms expressions used in index qualifications into standardized Var nodes that reference index columns. It handles two types of index columns:
1. Simple index columns (directly reference table columns)  
2. Index expressions (computed expressions stored as part of the index)

The function performs extensive sanity checking to ensure the given expression matches the expected index column. It removes any binary-compatible relabeling from the input node before processing. For simple columns, it verifies the Var node matches the expected table column and converts it to an INDEX_VAR reference. For expression-based index columns, it searches through the index expression list to find and validate the matching expression.

## Parameters / Member Variables
- : The indexqual expression node to be converted
- : IndexOptInfo structure containing information about the index
- : Zero-based index column position to convert the operand for

## Dependencies
- Functions called/Symbols referenced:
  - copyObject
  - makeVar
  - equal
  - list_head
  - lnext
  - exprType
  - exprCollation
  - INDEX_VAR
- Called from (representative examples):
  - fix_indexqual_clause

## Notes and Other Information
- The function uses INDEX_VAR as a special varno value to distinguish index column references from regular table column references
- Index column positions are 1-based in the resulting Var node (indexcol + 1) while the input indexcol parameter is 0-based
- Handles RelabelType nodes by unwrapping them to access the underlying expression
- Contains extensive error checking with elog(ERROR) calls for mismatched expressions
- This is a static function within createplan.c, part of the query plan creation process