# varchar_support

## Location
[src/backend/utils/adt/varchar.c:565-608](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varchar.c#L565-L608)

## Overview
Provides planner support for VARCHAR length coercion functions, optimizing cases where length constraints can be simplified or eliminated.

## Definition

```c
Datum
varchar_support(PG_FUNCTION_ARGS)
```
## Detailed Description
The `varchar_support` function serves as a planner support function for VARCHAR length coercion operations in PostgreSQL. It is called by the query planner to optimize expressions involving VARCHAR length constraints, particularly when converting between different VARCHAR length specifications.

The main optimization implemented is flattening calls that set a new maximum length that is greater than or equal to the previous maximum length. In such cases, the length coercion becomes redundant and can be simplified to just a type relabeling operation, eliminating unnecessary runtime overhead. The function ignores the `isExplicit` argument since it only affects truncation cases, which are not optimized away.

This function is part of PostgreSQL's planner support infrastructure that allows data types to provide custom optimization logic for their associated functions.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro providing access to:
  - Argument 0: `Node *rawreq` - Support request structure containing optimization context

## Dependencies
- Functions called/Symbols referenced:
  - `[SupportRequestSimplify](../S/SupportRequestSimplify.md)`: Structure type for simplification requests
  - `[FuncExpr](../F/FuncExpr.md)`: Function expression node type
  - `lsecond`: Macro to get the second element of a list
  - `[exprTypmod](../e/exprTypmod.md)`: Extracts type modifier from an expression
  - [DatumGetInt32](../D/DatumGetInt32.md): Converts Datum to int32 value
  - `[relabel_to_typmod](../r/relabel_to_typmod.md)`: Creates a relabeling node with new type modifier
  - `VARHDRSZ`: Constant representing variable header size

- Called from (representative examples):
  - PostgreSQL query planner during expression optimization
  - Function call simplification routines

## Notes and Other Information
- This function only handles `SupportRequestSimplify` request types currently
- The optimization eliminates redundant length checks when the new length limit is more permissive than the old one
- Type modifier calculations account for the variable header size (`VARHDRSZ`)
- Returns NULL when no optimization is possible, allowing the original expression to be used
- Part of PostgreSQL's extensible planner support system that allows types to define custom optimizations
- The `isExplicit` parameter is intentionally ignored as noted in the comments
- Registered as a support function in the PostgreSQL system catalogs for VARCHAR type operations

## Simplified Source

```c
Datum varchar_support(PG_FUNCTION_ARGS) {
    Node *rawreq = (Node *) PG_GETARG_POINTER(0);
    Node *ret = NULL;

    // Handle simplification requests only
    if (IsA(rawreq, SupportRequestSimplify)) {
        SupportRequestSimplify *req = (SupportRequestSimplify *) rawreq;
        FuncExpr *expr = req->fcall;

        // Get the new type modifier from second argument
        Node *typmod = (Node *) lsecond(expr->args);

        if (IsA(typmod, Const) && !((Const *) typmod)->constisnull) {
            Node *source = (Node *) linitial(expr->args);
            int32 old_typmod = exprTypmod(source);
            int32 new_typmod = DatumGetInt32(((Const *) typmod)->constvalue);

            // Calculate max lengths (accounting for header size)
            int32 old_max = old_typmod - VARHDRSZ;
            int32 new_max = new_typmod - VARHDRSZ;

            // Optimize: if new length >= old length, just relabel
            if (new_typmod < 0 || (old_typmod >= 0 && old_max <= new_max)) {
                ret = relabel_to_typmod(source, new_typmod);
            }
        }
    }

    PG_RETURN_POINTER(ret);
}
```