# transformRangeTableSample

## Location
[src/backend/parser/parse_clause.c:910-1012](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_clause.c#L910-L1012)

## Overview
Transforms a TABLESAMPLE clause into a TableSampleClause node, validating the sampling method, processing arguments with type coercion, and handling REPEATABLE specifications.

## Definition
static TableSampleClause *
transformRangeTableSample(ParseState *pstate, RangeTableSample *rts)

## Detailed Description
The transformRangeTableSample function handles the transformation of TABLESAMPLE clauses used for statistical sampling of table data. The function validates the tablesample method by looking up its handler function (which must accept one INTERNAL argument and return tsm_handler type), retrieves the TsmRoutine to get parameter type information, transforms and type-coerces all method arguments according to the expected parameter types, and processes the optional REPEATABLE clause (if supported by the method). The function ensures that the correct number of arguments are provided and that all expressions are properly transformed and assigned collations.

## Parameters / Member Variables
- pstate: ParseState structure containing the current parsing context and state information
- rts: RangeTableSample structure representing the raw TABLESAMPLE clause to be transformed, including method name, arguments, and optional REPEATABLE clause

## Dependencies
- Functions called/Symbols referenced:
  - [LookupFuncName](../L/LookupFuncName.md)
  - [get_func_rettype](../g/get_func_rettype.md)  
  - [GetTsmRoutine](../G/GetTsmRoutine.md)
  - makeNode
  - [transformExpr](transformExpr.md)
  - [coerce_to_specific_type](../c/coerce_to_specific_type.md)
  - [assign_expr_collations](../a/assign_expr_collations.md)
  - [NameListToString](../N/NameListToString.md)
  - TSM_HANDLEROID
  - EXPR_KIND_FROM_FUNCTION
  - FLOAT8OID
- Called from (representative examples):
  - [transformFromClauseItem](transformFromClauseItem.md)

## Notes and Other Information
- Tablesample method names are looked up as functions with specific signature: one INTERNAL argument returning tsm_handler type
- Schema qualification is allowed for tablesample method names to resolve ambiguity
- The function validates that the handler function returns TSM_HANDLEROID type
- Argument count validation ensures the provided arguments match the method's expected parameter count
- All arguments are transformed using EXPR_KIND_FROM_FUNCTION context and coerced to the expected parameter types
- REPEATABLE clause is optional and only supported by methods that set repeatable_across_queries = true
- REPEATABLE argument is always coerced to FLOAT8OID (double precision) type
- Collation assignment is performed immediately since assign_query_collations() doesn't examine RTE substructure
- Error messages distinguish between 'method does not exist' vs 'function does not exist' for better user experience
- The resulting TableSampleClause contains the handler OID, transformed arguments, and optional repeatable expression

## Simplified Source

```c
static TableSampleClause *transformRangeTableSample(ParseState *pstate, RangeTableSample *rts)
{
    TableSampleClause *tablesample;
    Oid handlerOid;
    Oid funcargtypes[1];
    TsmRoutine *tsm;
    List *fargs;

    // Look up handler function for the tablesample method
    funcargtypes[0] = INTERNALOID;
    handlerOid = LookupFuncName(rts->method, 1, funcargtypes, true);

    // Validate that the method exists
    if (!OidIsValid(handlerOid))
        ereport(ERROR, "tablesample method does not exist");

    // Check handler has correct return type (tsm_handler)
    if (get_func_rettype(handlerOid) != TSM_HANDLEROID)
        ereport(ERROR, "function must return type tsm_handler");

    // Get TsmRoutine for argument type information
    tsm = GetTsmRoutine(handlerOid);

    tablesample = makeNode(TableSampleClause);
    tablesample->tsmhandler = handlerOid;

    // Validate argument count matches method requirements
    if (list_length(rts->args) != list_length(tsm->parameterTypes))
        ereport(ERROR, "tablesample method requires different number of arguments");

    // Transform and type-coerce all method arguments
    fargs = NIL;
    forboth(larg, rts->args, ltyp, tsm->parameterTypes)
    {
        Node *arg = (Node *) lfirst(larg);
        Oid argtype = lfirst_oid(ltyp);

        arg = transformExpr(pstate, arg, EXPR_KIND_FROM_FUNCTION);
        arg = coerce_to_specific_type(pstate, arg, argtype, "TABLESAMPLE");
        assign_expr_collations(pstate, arg);
        fargs = lappend(fargs, arg);
    }
    tablesample->args = fargs;

    // Process optional REPEATABLE clause
    if (rts->repeatable != NULL)
    {
        Node *arg;

        // Check if method supports REPEATABLE
        if (!tsm->repeatable_across_queries)
            ereport(ERROR, "tablesample method does not support REPEATABLE");

        arg = transformExpr(pstate, rts->repeatable, EXPR_KIND_FROM_FUNCTION);
        arg = coerce_to_specific_type(pstate, arg, FLOAT8OID, "REPEATABLE");
        assign_expr_collations(pstate, arg);
        tablesample->repeatable = (Expr *) arg;
    }
    else
        tablesample->repeatable = NULL;

    return tablesample;
}
```