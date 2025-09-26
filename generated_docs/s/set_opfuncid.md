# set_opfuncid

## Location
[src/backend/nodes/nodeFuncs.c:1862-1872](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/nodeFuncs.c#L1862-L1872)

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
  - [get_opcode](../g/get_opcode.md)
  - InvalidOid (constant)
- Called from (representative examples):
  - [fix_opfuncids_walker](../f/fix_opfuncids_walker.md)
  - [check_functions_in_node](../c/check_functions_in_node.md)
  - [check_and_push_window_quals](../c/check_and_push_window_quals.md)
  - [cost_qual_eval_walker](../c/cost_qual_eval_walker.md)
  - [process_equivalence](../p/process_equivalence.md)
  - [match_opclause_to_indexcol](../m/match_opclause_to_indexcol.md)
  - [fix_expr_common](../f/fix_expr_common.md)
  - [eval_const_expressions_mutator](../e/eval_const_expressions_mutator.md)

## Notes and Other Information
- Can be used for DistinctExpr and NullIfExpr nodes due to struct equivalence
- Only sets opfuncid if it's currently InvalidOid, avoiding redundant lookups
- Part of PostgreSQL's expression processing infrastructure
- Essential for operator execution planning and optimization