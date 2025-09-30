# ParseFuncOrColumn

## Location
[src/backend/parser/parse_func.c:90-922](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_func.c#L90-L922)

## Overview
Parses a function call or column reference, handling both syntactic forms and resolving ambiguity between function calls and column projections in PostgreSQL's parser.

## Definition

```c
structs.  Don't do this if dealing with column syntax,
	 * nor if we had WITHIN GROUP (because in that case it's critical to keep
	 * the argument count unchanged).
	 */
	nargs = 0;
```
## Detailed Description
ParseFuncOrColumn is a central function in PostgreSQL's parser that handles the ambiguity between function calls and column references. PostgreSQL treats notations like 'tab.col' and 'col(tab)' as equivalent when possible - a single-argument function call with a complex type argument can be interpreted as column projection if the function name matches an attribute of the type.

The function performs several key operations:
1. Extracts and validates argument type information
2. Handles named arguments and validates their usage
3. Determines whether the construct could be a column projection
4. Calls func_get_detail to resolve the function in system catalogs
5. Validates function types (normal, aggregate, window, procedure, coercion)
6. Handles special cases for ordered-set and hypothetical aggregates
7. Enforces type consistency and performs necessary type casting
8. Builds appropriate output structures (FuncExpr, Aggref, WindowFunc)

## Parameters / Member Variables
- : ParseState containing parsing context and state information
- : List of names representing the function name (potentially schema-qualified)
- : List of already-transformed argument expressions
- : Copy of pstate->p_last_srf from before transforming fargs for SRF placement checking
- : FuncCall struct containing function decoration (NULL for column syntax)
- : Boolean indicating if this is a CALL statement requiring procedure resolution
- : Source location for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - [func_get_detail](../f/func_get_detail.md) (main function resolution)
  - [ParseComplexProjection](ParseComplexProjection.md) (for column projection handling)
  - [transformWhereClause](../t/transformWhereClause.md) (for aggregate filter processing)
  - [unify_hypothetical_args](../u/unify_hypothetical_args.md) (for hypothetical aggregate validation)
  - [enforce_generic_type_consistency](../e/enforce_generic_type_consistency.md) (for polymorphic type handling)
  - [make_fn_arguments](../m/make_fn_arguments.md) (for argument type casting)
  - [check_srf_call_placement](../c/check_srf_call_placement.md) (for set-returning function validation)
- Called from (representative examples):
  - [transformFuncCall](../t/transformFuncCall.md) (from parse_expr.c:1474)
  - [transformIndirection](../t/transformIndirection.md) (from parse_expr.c:480)
  - [transformCallStmt](../t/transformCallStmt.md) (from analyze.c:3112)

## Notes and Other Information
- The function supports both function syntax (fn != NULL) and column syntax (fn == NULL)
- For column syntax, returns NULL on failure rather than reporting errors
- Handles complex aggregate features like WITHIN GROUP, FILTER, and window functions
- Supports variadic functions with proper array construction for non-ANY variadics
- Enforces PostgreSQL's function argument limit (FUNC_MAX_ARGS)
- Special handling for procedures vs functions based on proc_call parameter
- Performs extensive validation for different function types and their allowed syntactic decorations

## Simplified Source

```c
Node *ParseFuncOrColumn(ParseState *pstate, List *funcname, List *fargs,
                       Node *last_srf, FuncCall *fn, bool proc_call, int location) {
    bool is_column = (fn == NULL);
    bool could_be_projection;
    Oid funcid, rettype;
    int nargs = 0;
    Oid actual_arg_types[FUNC_MAX_ARGS];
    FuncDetailCode fdresult;

    // Extract function decoration from FuncCall struct if present
    List *agg_order = (fn ? fn->agg_order : NIL);
    Expr *agg_filter = NULL;
    WindowDef *over = (fn ? fn->over : NULL);
    bool agg_within_group = (fn ? fn->agg_within_group : false);
    // ... other decorations

    // Transform aggregate filter if present
    if (fn && fn->agg_filter != NULL)
        agg_filter = (Expr *) transformWhereClause(pstate, fn->agg_filter,
                                                  EXPR_KIND_FILTER, "FILTER");

    // Check argument count limit
    if (list_length(fargs) > FUNC_MAX_ARGS)
        ereport(ERROR, (errcode(ERRCODE_TOO_MANY_ARGUMENTS),
                       errmsg("cannot pass more than %d arguments to a function", FUNC_MAX_ARGS)));

    // Extract argument types, filtering VOID Params for JDBC compatibility
    foreach(l, fargs) {
        Node *arg = lfirst(l);
        Oid argtype = exprType(arg);

        if (argtype == VOIDOID && IsA(arg, Param) && !is_column && !agg_within_group) {
            fargs = foreach_delete_current(fargs, l);
            continue;
        }
        actual_arg_types[nargs++] = argtype;
    }

    // Process named arguments and validate ordering
    argnames = extract_named_arguments(fargs);

    // Determine if this could be a column projection
    could_be_projection = (nargs == 1 && !proc_call &&
                          agg_order == NIL && agg_filter == NULL &&
                          !agg_star && !agg_distinct && over == NULL &&
                          !func_variadic && argnames == NIL &&
                          list_length(funcname) == 1 &&
                          (actual_arg_types[0] == RECORDOID || ISCOMPLEX(actual_arg_types[0])));

    // Try column projection first if using column syntax
    if (could_be_projection && is_column) {
        retval = ParseComplexProjection(pstate, strVal(linitial(funcname)),
                                       first_arg, location);
        if (retval)
            return retval;
    }

    // Main function resolution via catalog lookup
    fdresult = func_get_detail(funcname, fargs, argnames, nargs, actual_arg_types,
                              !func_variadic, true, proc_call,
                              &funcid, &rettype, &retset, &nvargs, &vatype,
                              &declared_arg_types, &argdefaults);

    // Handle different resolution results
    switch (fdresult) {
        case FUNCDETAIL_NORMAL:
        case FUNCDETAIL_PROCEDURE:
            // Validate no aggregate decorations for regular functions
            validate_not_aggregate_decorations();
            break;

        case FUNCDETAIL_AGGREGATE:
            // Handle aggregate-specific validation and setup
            handle_aggregate_function();
            break;

        case FUNCDETAIL_WINDOWFUNC:
            // Validate window function requirements
            if (!over)
                ereport(ERROR, "window function requires OVER clause");
            break;

        case FUNCDETAIL_COERCION:
            // Handle as type coercion
            return coerce_type(pstate, linitial(fargs), actual_arg_types[0],
                              rettype, -1, COERCION_EXPLICIT, COERCE_EXPLICIT_CALL, location);

        case FUNCDETAIL_MULTIPLE:
            // Handle ambiguous function matches
            report_ambiguous_function_error();
            break;

        default:
            // Function not found - try column projection or report error
            if (is_column)
                return NULL;
            if (could_be_projection) {
                retval = ParseComplexProjection(pstate, strVal(linitial(funcname)),
                                               first_arg, location);
                if (retval)
                    return retval;
            }
            report_function_not_found_error();
    }

    // Enforce polymorphic type consistency
    rettype = enforce_generic_type_consistency(actual_arg_types, declared_arg_types,
                                             nargsplusdefs, rettype, false);

    // Perform necessary argument type casting
    make_fn_arguments(pstate, fargs, actual_arg_types, declared_arg_types);

    // Handle variadic function call transformation
    if (nvargs > 0 && vatype != ANYOID) {
        transform_variadic_arguments();
    }

    // Validate set-returning function placement
    if (retset)
        check_srf_call_placement(pstate, last_srf, location);

    // Build appropriate output structure based on function type
    if (fdresult == FUNCDETAIL_NORMAL || fdresult == FUNCDETAIL_PROCEDURE) {
        FuncExpr *funcexpr = makeNode(FuncExpr);
        funcexpr->funcid = funcid;
        funcexpr->funcresulttype = rettype;
        funcexpr->funcretset = retset;
        funcexpr->args = fargs;
        funcexpr->location = location;
        retval = (Node *) funcexpr;
    }
    else if (fdresult == FUNCDETAIL_AGGREGATE && !over) {
        Aggref *aggref = makeNode(Aggref);
        aggref->aggfnoid = funcid;
        aggref->aggtype = rettype;
        aggref->aggfilter = agg_filter;
        aggref->location = location;
        transformAggregateCall(pstate, aggref, fargs, agg_order, agg_distinct);
        retval = (Node *) aggref;
    }
    else {
        // Window function
        WindowFunc *wfunc = makeNode(WindowFunc);
        wfunc->winfnoid = funcid;
        wfunc->wintype = rettype;
        wfunc->args = fargs;
        wfunc->location = location;
        transformWindowFuncCall(pstate, wfunc, over);
        retval = (Node *) wfunc;
    }

    // Update SRF tracking for higher-level error checks
    if (retset)
        pstate->p_last_srf = retval;

    return retval;
}
```