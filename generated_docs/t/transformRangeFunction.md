# transformRangeFunction

## Location
[src/backend/parser/parse_clause.c:465-687](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_clause.c#L465-L687)

## Overview
Transforms function calls appearing in FROM clauses into ParseNamespaceItems, handling special cases like multi-argument UNNEST() expansion and column definition lists.

## Definition
static ParseNamespaceItem *
transformRangeFunction(ParseState *pstate, RangeFunction *r)

## Detailed Description
The transformRangeFunction function handles the complex transformation of function calls that appear in FROM clauses. This function manages several intricate aspects: it automatically enables lateral references for all function calls (required for SQL spec compliance with UNNEST), handles the special case of multi-argument UNNEST() by expanding it into separate single-argument calls, processes column definition lists with proper validation, ensures set-returning functions appear at the top level, and manages collation assignment. The function also determines whether the RTE should be marked as LATERAL based on explicit specification or cross-references detection.

## Parameters / Member Variables
- pstate: ParseState structure containing the current parsing context and state information
- r: RangeFunction structure representing the function call(s) to be transformed, including function expressions, column definitions, lateral flag, and ordinality specification

## Dependencies
- Functions called/Symbols referenced:
  - [transformExpr](transformExpr.md)
  - [FigureColname](../F/FigureColname.md)
  - [makeFuncCall](../m/makeFuncCall.md)
  - SystemFuncName
  - [assign_list_collations](../a/assign_list_collations.md)
  - [contain_vars_of_level](../c/contain_vars_of_level.md)
  - [addRangeTableEntryForFunction](../a/addRangeTableEntryForFunction.md)
  - EXPR_KIND_FROM_FUNCTION
  - COERCE_EXPLICIT_CALL
- Called from (representative examples):
  - [transformFromClauseItem](transformFromClauseItem.md)

## Notes and Other Information
- The function automatically sets p_lateral_active = true for all function calls in FROM, regardless of explicit LATERAL marking
- Special handling for UNNEST() with multiple arguments: transforms unnest(a,b,c) into separate unnest(a), unnest(b), unnest(c) calls using pg_catalog.unnest
- Validates that set-returning functions appear at the top level of FROM clauses, enforcing nodeFunctionscan.c requirements
- Column definition lists are validated to prevent conflicts between per-function and top-level definitions
- Restrictions on column definition lists: only allowed with single functions and not with WITH ORDINALITY
- The function determines LATERAL marking based on either explicit specification (r->lateral) or presence of lateral cross-references
- Collation assignment is performed before creating the RTE to ensure proper collation information is available for Vars
- Error handling includes specific messages for ROWS FROM() vs UNNEST() syntax violations

## Simplified Source

```c
static ParseNamespaceItem *transformRangeFunction(ParseState *pstate, RangeFunction *r)
{
    List *funcexprs = NIL;
    List *funcnames = NIL;
    List *coldeflists = NIL;
    bool is_lateral;

    // Enable lateral references for SQL spec compliance
    pstate->p_lateral_active = true;

    // Transform each function expression
    foreach(lc, r->functions)
    {
        List *pair = (List *) lfirst(lc);
        Node *fexpr = (Node *) linitial(pair);
        List *coldeflist = (List *) lsecond(pair);
        Node *newfexpr;
        Node *last_srf;

        // Special case: expand multi-argument UNNEST() into separate calls
        if (IsA(fexpr, FuncCall))
        {
            FuncCall *fc = (FuncCall *) fexpr;

            // Check for unnest() with multiple args and no decoration
            if (list_length(fc->funcname) == 1 &&
                strcmp(strVal(linitial(fc->funcname)), "unnest") == 0 &&
                list_length(fc->args) > 1 &&
                /* ... other conditions ... */ &&
                coldeflist == NIL)
            {
                // Create separate unnest() call for each argument
                foreach(lc2, fc->args)
                {
                    Node *arg = (Node *) lfirst(lc2);
                    FuncCall *newfc;

                    last_srf = pstate->p_last_srf;
                    newfc = makeFuncCall(SystemFuncName("unnest"),
                                        list_make1(arg),
                                        COERCE_EXPLICIT_CALL,
                                        fc->location);

                    newfexpr = transformExpr(pstate, (Node *) newfc,
                                           EXPR_KIND_FROM_FUNCTION);

                    // Validate SRF at top level
                    if (pstate->p_last_srf != last_srf &&
                        pstate->p_last_srf != newfexpr)
                        ereport(ERROR, "set-returning functions must appear at top level of FROM");

                    funcexprs = lappend(funcexprs, newfexpr);
                    funcnames = lappend(funcnames, FigureColname((Node *) newfc));
                    coldeflists = lappend(coldeflists, coldeflist);
                }
                continue;
            }
        }

        // Normal function transformation
        last_srf = pstate->p_last_srf;
        newfexpr = transformExpr(pstate, fexpr, EXPR_KIND_FROM_FUNCTION);

        // Validate SRF at top level
        if (pstate->p_last_srf != last_srf &&
            pstate->p_last_srf != newfexpr)
            ereport(ERROR, "set-returning functions must appear at top level of FROM");

        funcexprs = lappend(funcexprs, newfexpr);
        funcnames = lappend(funcnames, FigureColname(fexpr));

        // Check for conflicting column definition lists
        if (coldeflist && r->coldeflist)
            ereport(ERROR, "multiple column definition lists are not allowed");

        coldeflists = lappend(coldeflists, coldeflist);
    }

    pstate->p_lateral_active = false;

    // Assign collations for proper type information
    assign_list_collations(pstate, funcexprs);

    // Handle top-level column definition list
    if (r->coldeflist)
    {
        if (list_length(funcexprs) != 1)
            ereport(ERROR, "column definition list requires single function");
        if (r->ordinality)
            ereport(ERROR, "WITH ORDINALITY cannot be used with column definition list");

        coldeflists = list_make1(r->coldeflist);
    }

    // Determine if LATERAL marking is needed
    is_lateral = r->lateral || contain_vars_of_level((Node *) funcexprs, 0);

    return addRangeTableEntryForFunction(pstate, funcnames, funcexprs,
                                        coldeflists, r, is_lateral, true);
}
```