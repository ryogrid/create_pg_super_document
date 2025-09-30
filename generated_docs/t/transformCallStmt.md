# transformCallStmt

## Location
[src/backend/parser/analyze.c:3088-3212](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/analyze.c#L3088-L3212)

## Overview
Transforms a CALL statement (procedure call) into a CMD_UTILITY Query node, handling argument expansion, parameter mode classification, and output argument separation.

## Definition

```c
static Query *
transformCallStmt(ParseState *pstate, CallStmt *stmt)
```
## Detailed Description
This function transforms a CALL statement used to invoke stored procedures. It performs comprehensive argument processing including transformation of input arguments, resolution of the called procedure, expansion of arguments to handle named parameters and defaults, and separation of arguments into input and output categories based on their parameter modes.

The transformation process includes:
1. Transforming each argument expression in the procedure call
2. Resolving the procedure using ParseFuncOrColumn to identify the target function
3. Expanding arguments to handle named parameters and default values (normally done during planning)
4. Retrieving the procedure's argument modes from the system catalog
5. Separating arguments into input arguments (IN, VARIADIC) and output arguments (OUT, INOUT)
6. Handling INOUT parameters that appear in both input and output argument lists

The function ensures that CALL statements can handle complex parameter scenarios including default values, named arguments, and mixed parameter modes.

## Parameters / Member Variables
- : Parse state containing context information for the transformation
- : The CALL statement to transform, containing:
  - : FuncCall node with procedure name and arguments
  - : Resolved function expression (set by this function)
  - : List of output arguments (set by this function)

## Dependencies
- Functions called/Symbols referenced:
  - [transformExpr](transformExpr.md), ParseFuncOrColumn, assign_expr_collations
  - [expand_function_arguments](../e/expand_function_arguments.md), SearchSysCache1, ReleaseSysCache
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md), DatumGetArrayTypeP, copyObject
  - [lappend](../l/lappend.md), lfirst, list_length, castNode, makeNode
  - HeapTupleIsValid, ObjectIdGetDatum
- Constants referenced:
  - EXPR_KIND_CALL_ARGUMENT, PROCOID, Anum_pg_proc_proargmodes
  - PROARGMODE_IN, PROARGMODE_OUT, PROARGMODE_INOUT, PROARGMODE_VARIADIC
  - CHAROID, CMD_UTILITY
- Array manipulation macros:
  - ARR_NDIM, ARR_DIMS, ARR_HASNULL, ARR_ELEMTYPE, ARR_DATA_PTR
- Called from (representative examples):
  - [transformStmt](transformStmt.md)

## Notes and Other Information
- Argument expansion occurs during parsing rather than planning, unlike regular function calls
- INOUT parameters are duplicated: original in input args, copy in output args
- PROARGMODE_TABLE parameters are not supported for procedures
- The function validates the proargmodes array structure to ensure proper format
- Output arguments include both OUT and INOUT parameters for result processing
- System catalog lookup is performed to get procedure metadata and argument modes
- The transformed function expression and output arguments are stored in the CallStmt for later execution

## Simplified Source

```c
static Query *transformCallStmt(ParseState *pstate, CallStmt *stmt) {
    List *targs = NIL;
    List *outargs = NIL;

    // Transform all argument expressions
    ListCell *lc;
    foreach(lc, stmt->funccall->args) {
        targs = lappend(targs, transformExpr(pstate, (Node *) lfirst(lc),
                                             EXPR_KIND_CALL_ARGUMENT));
    }

    // Resolve the procedure call
    Node *node = ParseFuncOrColumn(pstate, stmt->funccall->funcname, targs,
                                   pstate->p_last_srf, stmt->funccall,
                                   true, stmt->funccall->location);
    assign_expr_collations(pstate, node);
    FuncExpr *fexpr = castNode(FuncExpr, node);

    // Look up procedure in system catalog
    HeapTuple proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(fexpr->funcid));
    if (!HeapTupleIsValid(proctup))
        elog(ERROR, "cache lookup failed for function %u", fexpr->funcid);

    // Expand arguments for named parameters and defaults
    fexpr->args = expand_function_arguments(fexpr->args, true, fexpr->funcresulttype, proctup);

    // Get argument modes to separate input/output parameters
    bool isNull;
    Datum proargmodes = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_proargmodes, &isNull);

    if (!isNull) {
        // Parse argument modes array
        ArrayType *arr = DatumGetArrayTypeP(proargmodes);
        int numargs = list_length(fexpr->args);
        char *argmodes = (char *) ARR_DATA_PTR(arr);

        // Separate arguments by mode
        List *inargs = NIL;
        int i = 0;
        foreach(lc, fexpr->args) {
            Node *n = lfirst(lc);
            switch (argmodes[i]) {
                case PROARGMODE_IN:
                case PROARGMODE_VARIADIC:
                    inargs = lappend(inargs, n);
                    break;
                case PROARGMODE_OUT:
                    outargs = lappend(outargs, n);
                    break;
                case PROARGMODE_INOUT:
                    inargs = lappend(inargs, n);
                    outargs = lappend(outargs, copyObject(n));
                    break;
                default:
                    elog(ERROR, "invalid argmode %c for procedure", argmodes[i]);
            }
            i++;
        }
        fexpr->args = inargs;
    }

    // Store results in CallStmt
    stmt->funcexpr = fexpr;
    stmt->outargs = outargs;
    ReleaseSysCache(proctup);

    // Create utility Query node
    Query *result = makeNode(Query);
    result->commandType = CMD_UTILITY;
    result->utilityStmt = (Node *) stmt;

    return result;
}
```