# ExpandRowReference

## Location
[src/backend/parser/parse_target.c:1423-1518](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_target.c#L1423-L1518)

## Overview
Transforms a star expression (.*) applied to an arbitrary expression of composite type into a list of individual field expressions or target list entries.

## Definition

```c
static List *
ExpandRowReference(ParseState *pstate, Node *expr,
				   bool make_target_entry)
```
## Detailed Description
ExpandRowReference handles the expansion of star expressions when applied to complex composite-type expressions that are not simple table references. Unlike ExpandSingleTable which deals with simple table references, this function handles arbitrary expressions that evaluate to composite types.

The function operates in two main modes:
1. **Optimized path**: When the expression is a whole-row Var (varattno == InvalidAttrNumber), it delegates to ExpandSingleTable for efficient processing
2. **General path**: For arbitrary composite expressions, it creates multiple FieldSelect nodes by copying the original expression and selecting individual fields

For RECORD type variables, the function uses expandRecordVariable to determine the actual tuple structure, while other composite types use get_expr_result_tupdesc directly.

The function generates FieldSelect expressions for each non-dropped attribute, properly handling type information, type modifiers, and collation settings.

## Parameters / Member Variables
- : Parse state containing context information for the current parsing operation
- : The composite-type expression to be expanded (left side of .*)
- : Boolean flag determining whether to create TargetEntry structures (true) or simple FieldSelect expressions (false)

## Dependencies
- Functions called/Symbols referenced:
  - [ExpandSingleTable](ExpandSingleTable.md)
  - [GetNSItemByRangeTablePosn](../G/GetNSItemByRangeTablePosn.md)
  - [expandRecordVariable](../e/expandRecordVariable.md)
  - [get_expr_result_tupdesc](../g/get_expr_result_tupdesc.md)
  - makeNode
  - copyObject
  - [makeTargetEntry](../m/makeTargetEntry.md)
  - TupleDescAttr
  - InvalidAttrNumber
  - [ParseNamespaceItem](../P/ParseNamespaceItem.md)
  - [FieldSelect](../F/FieldSelect.md)
- Called from (representative examples):
  - [ExpandIndirectionStar](ExpandIndirectionStar.md)

## Notes and Other Information
- This is a static function within parse_target.c for internal target list processing
- The function includes an optimization for whole-row Vars that delegates to the more efficient ExpandSingleTable
- For complex expressions, the implementation creates multiple copies of the original expression, which can be inefficient for computationally expensive expressions
- Special handling is provided for RECORD type variables, which require runtime type resolution
- The function properly handles dropped columns by skipping them during expansion
- Permission checking is handled differently compared to ExpandSingleTable - for whole-row Vars, both table-level and column-level permissions may be marked

## Simplified Source

```c
static List *
ExpandRowReference(ParseState *pstate, Node *expr, bool make_target_entry)
{
    List *result = NIL;
    TupleDesc tupleDesc;
    int numAttrs;
    int i;

    // Optimization: if expr is a whole-row Var, delegate to ExpandSingleTable
    if (IsA(expr, Var) && ((Var *) expr)->varattno == InvalidAttrNumber)
    {
        Var *var = (Var *) expr;
        ParseNamespaceItem *nsitem;

        nsitem = GetNSItemByRangeTablePosn(pstate, var->varno, var->varlevelsup);
        return ExpandSingleTable(pstate, nsitem, var->varlevelsup, var->location, make_target_entry);
    }

    // Get tuple descriptor for the composite type expression
    if (IsA(expr, Var) && ((Var *) expr)->vartype == RECORDOID)
        tupleDesc = expandRecordVariable(pstate, (Var *) expr, 0);
    else
        tupleDesc = get_expr_result_tupdesc(expr, false);

    Assert(tupleDesc);

    // Generate FieldSelect expressions for each non-dropped attribute
    numAttrs = tupleDesc->natts;
    for (i = 0; i < numAttrs; i++)
    {
        Form_pg_attribute att = TupleDescAttr(tupleDesc, i);
        FieldSelect *fselect;

        if (att->attisdropped)
            continue;

        // Create FieldSelect node for this attribute
        fselect = makeNode(FieldSelect);
        fselect->arg = (Expr *) copyObject(expr);
        fselect->fieldnum = i + 1;
        fselect->resulttype = att->atttypid;
        fselect->resulttypmod = att->atttypmod;
        fselect->resultcollid = att->attcollation;

        if (make_target_entry)
        {
            // Wrap in TargetEntry if requested
            TargetEntry *te = makeTargetEntry((Expr *) fselect,
                                            (AttrNumber) pstate->p_next_resno++,
                                            pstrdup(NameStr(att->attname)),
                                            false);
            result = lappend(result, te);
        }
        else
            result = lappend(result, fselect);
    }

    return result;
}
```