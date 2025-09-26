# set_sa_opfuncid

## Location
src/backend/nodes/nodeFuncs.c: 1873 - 1899

## Overview
Sets the operator function ID (procedure OID) in a ScalarArrayOpExpr node if it hasn't been set already.

## Definition

```c
void
set_sa_opfuncid(ScalarArrayOpExpr *opexpr)
```
## Detailed Description
This function is the ScalarArrayOpExpr equivalent of set_opfuncid. It sets the opfuncid field in a ScalarArrayOpExpr node, which stores the OID of the procedure that implements the scalar array operator. Like set_opfuncid, it only sets the opfuncid if it is currently InvalidOid, preventing unnecessary lookups if the function ID has already been resolved. It uses get_opcode() to look up the procedure OID based on the operator OID stored in opno.

ScalarArrayOpExpr nodes represent operations like 'value = ANY(array)' or 'value <> ALL(array)', where a scalar value is compared against each element of an array using a specified operator.

## Parameters / Member Variables
- : Pointer to the ScalarArrayOpExpr node whose opfuncid field needs to be set

## Dependencies
- Functions called/Symbols referenced:
  - get_opcode
  - InvalidOid (constant)
- Called from (representative examples):
  - fix_opfuncids_walker
  - check_functions_in_node
  - cost_qual_eval_walker
  - fix_expr_common
  - is_strict_saop
  - eval_const_expressions_mutator

## Notes and Other Information
- Specialized version of set_opfuncid for ScalarArrayOpExpr nodes
- Only sets opfuncid if it's currently InvalidOid, avoiding redundant lookups
- Essential for processing array comparison operations (ANY/ALL constructs)
- Part of PostgreSQL's expression processing infrastructure for array operations