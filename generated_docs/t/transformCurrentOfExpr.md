# transformCurrentOfExpr

## Location
[src/backend/parser/parse_expr.c:2568-2619](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L2568-L2619)

## Overview
Transforms CURRENT OF expressions used in UPDATE/DELETE statements by resolving cursor references and optionally converting cursor names to parameter references for PL/pgSQL compatibility.

## Definition

```c
static Node *
transformCurrentOfExpr(ParseState *pstate, CurrentOfExpr *cexpr)
```
## Detailed Description
The  function processes CURRENT OF expressions that appear in UPDATE and DELETE statements. It first sets the range table index from the target relation in the parse state, then attempts to resolve cursor names as parameters.

The function includes a special optimization for PL/pgSQL: if the cursor name matches a REFCURSOR parameter, it converts the name reference to a parameter reference. This is done by creating a temporary ColumnRef and checking if parser hooks can resolve it to a REFCURSOR parameter. If successful, the cursor name is cleared and replaced with the parameter ID.

## Parameters / Member Variables
- `*pstate`: ParseState context containing target relation information and parser hooks
- `*cexpr`: Input CurrentOfExpr node containing cursor name or parameter information
## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [makeString](../m/makeString.md)
  - list_make1
  - IsA (macro)
  - PARAM_EXTERN (enum constant)
  - REFCURSOROID (OID constant)
- Called from (representative examples):
  - [transformExprRecurse](transformExprRecurse.md)

## Notes and Other Information
- Can only be used at the top level of UPDATE/DELETE statements (assertion check)
- Sets cvarno to the target relation's range table index
- Provides PL/pgSQL compatibility by converting cursor names to parameter references
- Uses parser hooks (p_pre_columnref_hook, p_post_columnref_hook) for cursor name resolution
- Silently ignores false matches that don't resolve to REFCURSOR parameters
- Modifies the input CurrentOfExpr node in place
- Located in src/backend/parser/parse_expr.c:2568-2619

## Simplified Source

```c
static Node *
transformCurrentOfExpr(ParseState *pstate, CurrentOfExpr *cexpr)
{
    // CURRENT OF can only appear at top level of UPDATE/DELETE
    Assert(pstate->p_target_nsitem != NULL);
    cexpr->cvarno = pstate->p_target_nsitem->p_rtindex;

    // Check if cursor name matches a REFCURSOR parameter (PL/pgSQL compatibility)
    if (cexpr->cursor_name != NULL)
    {
        // Create a temporary ColumnRef for the cursor name
        ColumnRef *cref = makeNode(ColumnRef);
        cref->fields = list_make1(makeString(cexpr->cursor_name));
        cref->location = -1;

        // Try to resolve through parser hooks
        Node *node = NULL;
        if (pstate->p_pre_columnref_hook != NULL)
            node = pstate->p_pre_columnref_hook(pstate, cref);
        if (node == NULL && pstate->p_post_columnref_hook != NULL)
            node = pstate->p_post_columnref_hook(pstate, cref, NULL);

        // If resolved to a REFCURSOR parameter, convert to parameter reference
        if (node != NULL && IsA(node, Param))
        {
            Param *p = (Param *) node;
            if (p->paramkind == PARAM_EXTERN && p->paramtype == REFCURSOROID)
            {
                cexpr->cursor_name = NULL;
                cexpr->cursor_param = p->paramid;
            }
        }
    }

    return (Node *) cexpr;
}
```