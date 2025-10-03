# inline_set_returning_function

## Location
[src/backend/optimizer/util/clauses.c:5065-5357](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L5065-L5357)

## Overview
Attempts to inline a set-returning SQL function in the FROM clause by expanding the function body and returning a substitute Query structure, enabling optimization of set-returning function calls.

## Definition

```c
Query *
inline_set_returning_function(PlannerInfo *root, RangeTblEntry *rte)
```
## Detailed Description
This function performs inline expansion of set-returning SQL functions that appear in range table entries (FROM clause). It analyzes whether a given function call can be safely inlined and, if so, parses and processes the function body to create a substitute Query structure that replaces the function call. The inlining process involves extensive validation to ensure the substitution is safe and semantically equivalent to the original function call.

The function performs several critical safety checks: verifying that the function is SQL-language, not strict, not volatile, doesn't have security definer properties, and returns a set. It also ensures that arguments don't contain volatile functions or subplans that could change behavior when evaluated multiple times. The function handles both traditional prosrc-based function definitions and newer prosqlbody-based definitions.

The inlining process involves parsing the function body, analyzing and rewriting it with proper parameter substitution, and validating that the result type matches the declared function signature. Special attention is given to composite return types and tuple result validation. The function creates appropriate memory contexts for temporary allocations and sets up error callbacks to provide meaningful error messages during the inlining process.

## Parameters / Member Variables
- `*root`: PlannerInfo containing global information about the query being planned
- `*rte`: RangeTblEntry representing the function call in the FROM clause (must be RTE_FUNCTION type)
## Dependencies
- Functions called/Symbols referenced:
  -  - prevents infinite recursion in self-referential functions
  -  - checks for volatile functions in arguments
  -  - checks for subplans in arguments
  -  - verifies execute permissions on the function
  -  - checks if function has entry/exit hooks
  -  - checks for NULL attributes in pg_proc tuple
  -  - sets up parameter information for parsing
  -  - parses the function body SQL
  -  - analyzes and rewrites the parsed query
  -  - configures parser hooks for SQL functions
  -  - validates function return type compatibility
  -  - replaces parameters with actual arguments
  -  - records plan dependency on the function
  -  - [error](../e/error.md) callback for enhanced error reporting

- Called from (representative examples):
  -  - during query preprocessing to inline eligible functions

## Notes and Other Information
- Only processes RTE_FUNCTION entries that represent single, simple FuncExpr nodes
- Fails for functions with ORDINALITY (WITH ORDINALITY clause)
- Requires functions to be SQL-language, not strict, not volatile, and declared as set-returning
- Creates temporary memory contexts to avoid memory leaks during parsing and processing
- Handles both prosrc (traditional) and prosqlbody (newer) function body storage formats
- Performs extensive type checking to ensure inlined query matches declared function signature
- For composite return types, validates that the function returns complete tuples rather than individual composite values
- Records dependencies and row-level security requirements from the inlined query
- Returns NULL if inlining is not possible or safe, allowing fallback to regular function execution
- The inlined query replaces the original function call, potentially enabling further optimizations by the planner

## Simplified Source

```c
Query *
inline_set_returning_function(PlannerInfo *root, RangeTblEntry *rte)
{
    // Basic validation - must be RTE_FUNCTION with single FuncExpr
    Assert(rte->rtekind == RTE_FUNCTION);
    check_stack_depth();  // Prevent infinite recursion

    if (rte->funcordinality || list_length(rte->functions) != 1)
        return NULL;

    FuncExpr *fexpr = (FuncExpr *) ((RangeTblFunction *) linitial(rte->functions))->funcexpr;
    if (!IsA(fexpr, FuncExpr) || !fexpr->funcretset)
        return NULL;

    // Safety checks - reject volatile functions or subplans in arguments
    if (contain_volatile_functions((Node *) fexpr->args) ||
        contain_subplans((Node *) fexpr->args))
        return NULL;

    // Permission and hook checks
    if (object_aclcheck(ProcedureRelationId, fexpr->funcid, GetUserId(), ACL_EXECUTE) != ACLCHECK_OK ||
        FmgrHookIsNeeded(fexpr->funcid))
        return NULL;

    // Validate function properties from pg_proc
    HeapTuple func_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(fexpr->funcid));
    Form_pg_proc funcform = (Form_pg_proc) GETSTRUCT(func_tuple);

    // Must be SQL function, not strict, not volatile, returns set
    if (funcform->prolang != SQLlanguageId ||
        funcform->proisstrict ||
        funcform->provolatile == PROVOLATILE_VOLATILE ||
        !funcform->proretset)
    {
        ReleaseSysCache(func_tuple);
        return NULL;
    }

    // Create temporary memory context for parsing
    MemoryContext mycxt = AllocSetContextCreate(CurrentMemoryContext,
                                               "inline_set_returning_function",
                                               ALLOCSET_DEFAULT_SIZES);
    MemoryContext oldcxt = MemoryContextSwitchTo(mycxt);

    Query *querytree = NULL;

    // Parse function body (handle both prosqlbody and prosrc)
    if (/* has prosqlbody */) {
        // Use pre-parsed query tree
        querytree = /* process prosqlbody */;
    } else {
        // Parse prosrc text
        char *src = /* get function source */;
        List *raw_parsetree_list = pg_parse_query(src);

        if (list_length(raw_parsetree_list) != 1)
            goto fail;

        List *querytree_list = pg_analyze_and_rewrite_withcb(/* parse and analyze */);
        if (list_length(querytree_list) != 1)
            goto fail;

        querytree = linitial(querytree_list);
    }

    // Validate result - must be SELECT query
    if (!IsA(querytree, Query) || querytree->commandType != CMD_SELECT)
        goto fail;

    // Validate return type compatibility
    if (!check_sql_fn_retval(/* validate return type */))
        goto fail;

    // Substitute actual parameters into the query
    querytree = substitute_actual_srf_parameters(querytree,
                                                funcform->pronargs,
                                                fexpr->args);

    // Copy result and cleanup
    MemoryContextSwitchTo(oldcxt);
    querytree = copyObject(querytree);
    MemoryContextDelete(mycxt);
    ReleaseSysCache(func_tuple);

    // Record plan dependencies
    record_plan_function_dependency(root, fexpr->funcid);
    if (querytree->hasRowSecurity)
        root->glob->dependsOnRole = true;

    return querytree;

fail:
    MemoryContextSwitchTo(oldcxt);
    MemoryContextDelete(mycxt);
    ReleaseSysCache(func_tuple);
    return NULL;
}
```