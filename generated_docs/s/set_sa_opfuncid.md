# set_sa_opfuncid

## Location
[src/backend/nodes/nodeFuncs.c:1873-1899](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/nodeFuncs.c#L1873-L1899)

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
- `*opexpr`: Pointer to the ScalarArrayOpExpr node whose opfuncid field needs to be set
## Dependencies
- Functions called/Symbols referenced:
  - [get_opcode](../g/get_opcode.md)
  - InvalidOid (constant)
- Called from (representative examples):
  - [fix_opfuncids_walker](../f/fix_opfuncids_walker.md)
  - [check_functions_in_node](../c/check_functions_in_node.md)
  - [cost_qual_eval_walker](../c/cost_qual_eval_walker.md)
  - [fix_expr_common](../f/fix_expr_common.md)
  - [is_strict_saop](../i/is_strict_saop.md)
  - [eval_const_expressions_mutator](../e/eval_const_expressions_mutator.md)

## Notes and Other Information
- Specialized version of set_opfuncid for ScalarArrayOpExpr nodes
- Only sets opfuncid if it's currently InvalidOid, avoiding redundant lookups
- Essential for processing array comparison operations (ANY/ALL constructs)
- Part of PostgreSQL's expression processing infrastructure for array operations

## Simplified Source

```c
void
set_sa_opfuncid(ScalarArrayOpExpr *opexpr)
{
    // Only set if not already set (avoid redundant lookups)
    if (opexpr->opfuncid == InvalidOid)
        opexpr->opfuncid = get_opcode(opexpr->opno);
}
```