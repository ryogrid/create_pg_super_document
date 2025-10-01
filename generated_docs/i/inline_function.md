# inline_function

## Location
[src/backend/optimizer/util/clauses.c:4551-4906](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L4551-L4906)

## Overview
Attempts to expand a SQL function call inline by substituting the function body directly into the calling query, avoiding function call overhead and exposing optimization opportunities.

## Definition

```c
structs and must use all of the function
	 * parameters (this is overkill, but an exact analysis is hard).
	 */
	if (funcform->provolatile == PROVOLATILE_IMMUTABLE &&
		contain_mutable_functions(newexpr))
		goto fail;
```
## Detailed Description
This function performs function inlining optimization for SQL-language functions. It attempts to replace function calls with their actual implementation when the function body is a simple "SELECT expression". This optimization eliminates the per-call overhead of SQL functions and can expose additional constant-folding opportunities.

The function includes comprehensive safety checks to prevent problematic inlining scenarios: recursive functions (tracked via context->active_fns), functions with multiple parameter usage of volatile/expensive expressions, functions that would change volatility/strictness properties, and functions with context-dependent nodes. It parses the function body, validates it's a simple SELECT statement, performs parameter substitution, and recursively optimizes the result.

The inlining process involves several phases: validation of function properties, parsing the function source code (handling both prosrc and prosqlbody), parameter substitution with usage counting, cost analysis for multiply-used parameters, and final collation handling.

## Parameters / Member Variables
- : OID of the function to inline
- : Expected result type OID of the function
- : Collation ID for the result
- : Collation ID for the inputs
- : List of actual function arguments
- : Whether the function is variadic
- : HeapTuple containing the function's catalog entry
- : Evaluation context containing active function tracking and optimization settings

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_proc (function catalog entry structure)
  - [heap_attisnull](../h/heap_attisnull.md) (checks for NULL attributes)
  - [prepare_sql_fn_parse_info](../p/prepare_sql_fn_parse_info.md) (prepares SQL function parsing context)
  - [pg_parse_query](../p/pg_parse_query.md) (parses SQL text into parse trees)
  - [sql_fn_parser_setup](../s/sql_fn_parser_setup.md) (configures parser for SQL functions)
  - [transformTopLevelStmt](../t/transformTopLevelStmt.md) (transforms parse tree to Query)
  - [check_sql_fn_retval](../c/check_sql_fn_retval.md) (validates function return value)
  - [substitute_actual_parameters](../s/substitute_actual_parameters.md) (replaces parameter references)
  - [contain_volatile_functions](../c/contain_volatile_functions.md), contain_mutable_functions, contain_nonstrict_functions (volatility checks)
  - [eval_const_expressions_mutator](../e/eval_const_expressions_mutator.md) (recursive optimization)
- Called from:
  - [simplify_function](../s/simplify_function.md) (main function simplification routine)

## Notes and Other Information
- Returns NULL if inlining is not possible, otherwise returns the inlined expression
- Only inlines SQL-language functions that are simple SELECT expressions
- Prevents recursive inlining by tracking active functions in context
- Uses temporary memory context to avoid leaks during parsing
- Enforces parameter usage rules: strict functions must use all parameters, expensive/volatile parameters cannot be used multiple times
- Handles both prosrc (text) and prosqlbody (parsed) function representations
- Records plan dependency on inlined functions for proper invalidation
- Maintains proper collation information in the result
- Located in src/backend/optimizer/util/clauses.c at lines 4551-4906

## Simplified Source

```c
static Expr *
inline_function(Oid funcid, Oid result_type, Oid result_collid,
                Oid input_collid, List *args, bool funcvariadic,
                HeapTuple func_tuple, eval_const_expressions_context *context)
{
    Form_pg_proc funcform = (Form_pg_proc) GETSTRUCT(func_tuple);
    char *src;
    MemoryContext oldcxt, mycxt;
    Query *querytree;
    Node *newexpr;
    int *usecounts;
    int i;

    // Basic function validation - must be SQL language, not security definer, etc.
    if (funcform->prolang != SQLlanguageId ||
        funcform->prokind != PROKIND_FUNCTION ||
        funcform->prosecdef ||
        funcform->proretset ||
        funcform->prorettype == RECORDOID ||
        !heap_attisnull(func_tuple, Anum_pg_proc_proconfig, NULL) ||
        funcform->pronargs != list_length(args))
        return NULL;

    // Prevent recursive inlining
    if (list_member_oid(context->active_fns, funcid))
        return NULL;

    // Check permissions and plugin hooks
    if (object_aclcheck(ProcedureRelationId, funcid, GetUserId(), ACL_EXECUTE) != ACLCHECK_OK ||
        FmgrHookIsNeeded(funcid))
        return NULL;

    // Create temporary memory context for parsing
    mycxt = AllocSetContextCreate(CurrentMemoryContext, "inline_function", ALLOCSET_DEFAULT_SIZES);
    oldcxt = MemoryContextSwitchTo(mycxt);

    // Parse function body (either from prosrc or prosqlbody)
    Datum tmp = SysCacheGetAttr(PROCOID, func_tuple, Anum_pg_proc_prosqlbody, &isNull);
    if (!isNull) {
        // Use pre-parsed body if available
        Node *n = stringToNode(TextDatumGetCString(tmp));
        querytree = linitial(IsA(n, List) ? linitial_node(List, castNode(List, n)) : list_make1(n));
    } else {
        // Parse from source text
        tmp = SysCacheGetAttrNotNull(PROCOID, func_tuple, Anum_pg_proc_prosrc);
        src = TextDatumGetCString(tmp);

        List *raw_parsetree_list = pg_parse_query(src);
        if (list_length(raw_parsetree_list) != 1)
            goto fail;

        ParseState *pstate = make_parsestate(NULL);
        pstate->p_sourcetext = src;
        sql_fn_parser_setup(pstate, prepare_sql_fn_parse_info(func_tuple, (Node *) fexpr, input_collid));
        querytree = transformTopLevelStmt(pstate, linitial(raw_parsetree_list));
        free_parsestate(pstate);
    }

    // Validate it's a simple SELECT expression (no aggregates, subqueries, etc.)
    if (!IsA(querytree, Query) ||
        querytree->commandType != CMD_SELECT ||
        querytree->hasAggs || querytree->hasWindowFuncs || querytree->hasSubLinks ||
        querytree->rtable || querytree->jointree->fromlist ||
        list_length(querytree->targetList) != 1)
        goto fail;

    // Validate return type compatibility
    if (check_sql_fn_retval(list_make1(list_make1(querytree)), result_type, NULL,
                           funcform->prokind, false, NULL))
        goto fail;

    // Extract the target expression
    newexpr = (Node *) ((TargetEntry *) linitial(querytree->targetList))->expr;

    // Check volatility and strictness constraints
    if ((funcform->provolatile == PROVOLATILE_IMMUTABLE && contain_mutable_functions(newexpr)) ||
        (funcform->provolatile == PROVOLATILE_STABLE && contain_volatile_functions(newexpr)) ||
        (funcform->proisstrict && contain_nonstrict_functions(newexpr)) ||
        contain_context_dependent_node((Node *) args))
        goto fail;

    // Substitute parameters and check usage patterns
    usecounts = (int *) palloc0(funcform->pronargs * sizeof(int));
    newexpr = substitute_actual_parameters(newexpr, funcform->pronargs, args, usecounts);

    // Validate parameter usage (strict functions need all params, expensive params can't be used multiple times)
    for (i = 0; i < funcform->pronargs; i++) {
        if (usecounts[i] == 0 && funcform->proisstrict)
            goto fail;
        if (usecounts[i] > 1) {
            Node *param = list_nth(args, i);
            if (contain_subplans(param) || contain_volatile_functions(param))
                goto fail;
            // Check if parameter is too expensive to evaluate multiple times
            QualCost eval_cost;
            cost_qual_eval(&eval_cost, list_make1(param), NULL);
            if (eval_cost.startup + eval_cost.per_tuple > 10 * cpu_operator_cost)
                goto fail;
        }
    }

    // Success - copy result and clean up
    MemoryContextSwitchTo(oldcxt);
    newexpr = copyObject(newexpr);
    MemoryContextDelete(mycxt);

    // Handle collation if needed
    if (OidIsValid(result_collid)) {
        Oid exprcoll = exprCollation(newexpr);
        if (OidIsValid(exprcoll) && exprcoll != result_collid) {
            CollateExpr *newnode = makeNode(CollateExpr);
            newnode->arg = (Expr *) newexpr;
            newnode->collOid = result_collid;
            newnode->location = -1;
            newexpr = (Node *) newnode;
        }
    }

    // Record dependency and recursively optimize
    if (context->root)
        record_plan_function_dependency(context->root, funcid);

    context->active_fns = lappend_oid(context->active_fns, funcid);
    newexpr = eval_const_expressions_mutator(newexpr, context);
    context->active_fns = list_delete_last(context->active_fns);

    return (Expr *) newexpr;

fail:
    MemoryContextSwitchTo(oldcxt);
    MemoryContextDelete(mycxt);
    return NULL;
}
```