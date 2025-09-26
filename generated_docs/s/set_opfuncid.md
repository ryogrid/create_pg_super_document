# set_opfuncid

## Location
src/backend/nodes/nodeFuncs.c: 1862 - 1872

## Overview
Sets the operator function ID (procedure OID) in an OpExpr node if it hasn't been set already.

## Definition

```c
void
set_opfuncid(OpExpr *opexpr)
```
## Detailed Description
This function sets the opfuncid field in an OpExpr node, which stores the OID of the procedure that implements the operator. The function only sets the opfuncid if it is currently InvalidOid, preventing unnecessary lookups if the function ID has already been resolved. It uses get_opcode() to look up the procedure OID based on the operator OID stored in opno.

Due to struct equivalence, this function can also be used for DistinctExpr and NullIfExpr nodes, which have the same memory layout as OpExpr for the relevant fields.

## Parameters / Member Variables
- : Pointer to the OpExpr node whose opfuncid field needs to be set

## Dependencies
- Functions called/Symbols referenced:
  - get_opcode
  - InvalidOid (constant)
- Called from (representative examples):
  - fix_opfuncids_walker
  - check_functions_in_node
  - check_and_push_window_quals
  - cost_qual_eval_walker
  - process_equivalence
  - match_opclause_to_indexcol
  - fix_expr_common
  - eval_const_expressions_mutator

## Notes and Other Information
- Can be used for DistinctExpr and NullIfExpr nodes due to struct equivalence
- Only sets opfuncid if it's currently InvalidOid, avoiding redundant lookups
- Part of PostgreSQL's expression processing infrastructure
- Essential for operator execution planning and optimization