# strip_implicit_coercions

## Location
[src/backend/nodes/nodeFuncs.c:700-757](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/nodeFuncs.c#L700-L757)

## Overview
Removes implicit coercions at the top level of an expression tree without modifying or copying the input, returning a pointer to the appropriate sub-expression.

## Definition

```c
Node *
strip_implicit_coercions(Node *node)
```
## Detailed Description
This function recursively traverses down expression trees to remove implicit type coercions that were inserted by PostgreSQL's type system. It handles various types of coercion nodes including function calls, relabel operations, I/O-based coercions, array coercions, row type conversions, and domain coercions. The function only removes coercions marked with  format, leaving explicit coercions intact. It returns a pointer to a location within the original tree rather than creating copies, making it efficient for cases where the original structure needs to be preserved.

## Parameters / Member Variables
- `*node`: The root node of the expression tree from which to strip implicit coercions. Can be NULL.
## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for type checking)
  - linitial (macro for accessing first list element)
  - [FuncExpr](../F/FuncExpr.md) (function call expression node)
  - [RelabelType](../R/RelabelType.md) (type relabeling node)
  - [CoerceViaIO](../C/CoerceViaIO.md) (I/O-based coercion node)
  - [ArrayCoerceExpr](../A/ArrayCoerceExpr.md) (array coercion expression)
  - [ConvertRowtypeExpr](../C/ConvertRowtypeExpr.md) (row type conversion expression)
  - [CoerceToDomain](../C/CoerceToDomain.md) (domain coercion node)
  - COERCE_IMPLICIT_CAST (coercion format constant)

- Called from (representative examples):
  - [ATExecAlterColumnType](../A/ATExecAlterColumnType.md) (table alteration)
  - [findTargetlistEntrySQL99](../f/findTargetlistEntrySQL99.md) (SQL99 target list parsing)
  - [AcquireRewriteLocks](../A/AcquireRewriteLocks.md) (query rewriting)
  - [get_update_query_targetlist_def](../g/get_update_query_targetlist_def.md) (rule utilities)
  - [get_rule_expr](../g/get_rule_expr.md) (rule expression formatting)

## Notes and Other Information
- The function does not modify the input expression tree, making it safe to use in contexts where the original structure must be preserved
- RowExpr nodes are returned unchanged even if marked as implicit coercions, as there's no meaningful way to strip them
- The function is recursive, continuing to strip nested implicit coercions until it reaches a non-coercion node
- This is commonly used in query planning and rewriting phases where the actual underlying expressions are needed without the coercion wrapper nodes

## Simplified Source

```c
Node *
strip_implicit_coercions(Node *node)
{
    if (node == NULL)
        return NULL;

    // Handle function call coercions
    if (IsA(node, FuncExpr)) {
        FuncExpr *f = (FuncExpr *) node;
        if (f->funcformat == COERCE_IMPLICIT_CAST)
            return strip_implicit_coercions(linitial(f->args));
    }

    // Handle type relabeling coercions
    else if (IsA(node, RelabelType)) {
        RelabelType *r = (RelabelType *) node;
        if (r->relabelformat == COERCE_IMPLICIT_CAST)
            return strip_implicit_coercions((Node *) r->arg);
    }

    // Handle I/O-based coercions
    else if (IsA(node, CoerceViaIO)) {
        CoerceViaIO *c = (CoerceViaIO *) node;
        if (c->coerceformat == COERCE_IMPLICIT_CAST)
            return strip_implicit_coercions((Node *) c->arg);
    }

    // Handle array element coercions
    else if (IsA(node, ArrayCoerceExpr)) {
        ArrayCoerceExpr *c = (ArrayCoerceExpr *) node;
        if (c->coerceformat == COERCE_IMPLICIT_CAST)
            return strip_implicit_coercions((Node *) c->arg);
    }

    // Handle row type conversions
    else if (IsA(node, ConvertRowtypeExpr)) {
        ConvertRowtypeExpr *c = (ConvertRowtypeExpr *) node;
        if (c->convertformat == COERCE_IMPLICIT_CAST)
            return strip_implicit_coercions((Node *) c->arg);
    }

    // Handle domain coercions
    else if (IsA(node, CoerceToDomain)) {
        CoerceToDomain *c = (CoerceToDomain *) node;
        if (c->coercionformat == COERCE_IMPLICIT_CAST)
            return strip_implicit_coercions((Node *) c->arg);
    }

    // No implicit coercion found - return original node
    return node;
}
```