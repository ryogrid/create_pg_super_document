# generate_function_name

## Location
[src/backend/utils/adt/ruleutils.c:12927-13031](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L12927-L13031)

## Overview
Computes the properly qualified and quoted name to display for a function specified by OID, considering argument types, variadic behavior, and function resolution rules to determine if schema qualification is needed.

## Definition

```c
static char *
generate_function_name(Oid funcid, int nargs, List *argnames, Oid *argtypes,
					   bool has_variadic, bool *use_variadic_p,
					   bool inGroupBy)
```
## Detailed Description
This function generates an appropriate display name for a function call, implementing sophisticated logic to determine whether schema qualification is necessary. It considers function overloading resolution rules by checking if the unqualified function name with the given arguments would resolve to the same function. The function also handles variadic functions properly, determining whether the VARIADIC keyword should be displayed, and includes special handling for functions like "cube" and "rollup" that require qualification in GROUP BY contexts due to parser limitations.

## Parameters / Member Variables
- `funcid`: The OID of the function to generate a name for
- `nargs`: The number of arguments being passed to the function
- `argnames`: List of argument names (can be NIL if no named arguments)
- `argtypes`: Array of argument type OIDs
- `has_variadic`: True if variadic arguments have been merged into an array
- `use_variadic_p`: Output parameter set to indicate whether VARIADIC should be printed; can be NULL for non-FuncExpr cases
- `inGroupBy`: True if generating the name for use in a GROUP BY clause

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - NameStr
  - [func_get_detail](../f/func_get_detail.md)
  - [makeString](../m/makeString.md)
  - list_make1
  - [get_namespace_name_or_temp](get_namespace_name_or_temp.md)
  - [quote_qualified_identifier](../q/quote_qualified_identifier.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - elog
- Called from (representative examples):
  - [pg_get_triggerdef_worker](../p/pg_get_triggerdef_worker.md)
  - [pg_get_functiondef](../p/pg_get_functiondef.md)
  - [get_func_expr](get_func_expr.md)
  - [get_agg_expr_helper](get_agg_expr_helper.md)
  - [get_windowfunc_expr_helper](get_windowfunc_expr_helper.md)
  - [get_tablesample_def](get_tablesample_def.md)

## Notes and Other Information
- This is a static function local to ruleutils.c
- Implements intelligent qualification logic based on function resolution rules
- Handles the complexity of PostgreSQL's function overloading system
- Special cases exist for "cube" and "rollup" functions in GROUP BY contexts
- Critical for generating correct SQL when functions might be overloaded
- The returned string is palloc'd and must be freed by the caller
- Part of PostgreSQL's expression deparsing infrastructure

## Simplified Source

```c
static char *
generate_function_name(Oid funcid, int nargs, List *argnames, Oid *argtypes,
                       bool has_variadic, bool *use_variadic_p, bool inGroupBy)
{
    HeapTuple proctup;
    Form_pg_proc procform;
    char *proname;
    char *nspname;
    bool use_variadic;
    bool force_qualify = false;

    // Look up function in system catalog
    proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(proctup))
        elog(ERROR, "cache lookup failed for function %u", funcid);

    procform = (Form_pg_proc) GETSTRUCT(proctup);
    proname = NameStr(procform->proname);

    // Force qualification for cube/rollup in GROUP BY
    if (inGroupBy) {
        if (strcmp(proname, "cube") == 0 || strcmp(proname, "rollup") == 0)
            force_qualify = true;
    }

    // Determine if VARIADIC should be printed
    if (use_variadic_p) {
        use_variadic = has_variadic;
        *use_variadic_p = use_variadic;
    } else {
        use_variadic = false;
    }

    // Check if schema qualification is needed
    if (!force_qualify) {
        FuncDetailCode p_result;
        Oid p_funcid;
        Oid p_rettype;
        bool p_retset;
        int p_nvargs;
        Oid p_vatype;
        Oid *p_true_typeids;

        p_result = func_get_detail(list_make1(makeString(proname)),
                                   NIL, argnames, nargs, argtypes,
                                   !use_variadic, true, false,
                                   &p_funcid, &p_rettype, &p_retset,
                                   &p_nvargs, &p_vatype, &p_true_typeids, NULL);

        // Don't qualify if lookup finds the same function
        if ((p_result == FUNCDETAIL_NORMAL ||
             p_result == FUNCDETAIL_AGGREGATE ||
             p_result == FUNCDETAIL_WINDOWFUNC) && p_funcid == funcid)
            nspname = NULL;
        else
            nspname = get_namespace_name_or_temp(procform->pronamespace);
    } else {
        nspname = get_namespace_name_or_temp(procform->pronamespace);
    }

    char *result = quote_qualified_identifier(nspname, proname);
    ReleaseSysCache(proctup);
    return result;
}
```