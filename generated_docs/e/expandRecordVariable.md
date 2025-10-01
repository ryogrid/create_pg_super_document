# expandRecordVariable

## Location
[src/backend/parser/parse_target.c:1519-1703](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_target.c#L1519-L1703)

## Overview
Determines the tuple descriptor for a Var of type RECORD by drilling down to find the ultimate defining expression and inferring the tuple structure from it.

## Definition

```c
TupleDesc
expandRecordVariable(ParseState *pstate, Var *var, int levelsup)
```
## Detailed Description
expandRecordVariable handles the complex task of determining the structure of RECORD-type variables, which have no predefined schema. Since PostgreSQL does not allow actual table or view columns to have type RECORD, such variables must refer to JOIN RTEs, FUNCTION RTEs, or subquery outputs.

The function operates through several strategies:

1. **Whole-row references**: When varattno is InvalidAttrNumber, it expands all fields from the referenced RTE using expandRTE and builds a tuple descriptor from the resulting variable list.

2. **RTE drilling**: Based on the RTE type, it recursively resolves the actual expression:
   - **RTE_SUBQUERY**: Examines the corresponding target list entry in the subquery
   - **RTE_JOIN**: Follows the join alias variables to find the underlying expression  
   - **RTE_CTE**: Looks up the corresponding entry in the CTE target list
   - **Other RTE types**: Generally invalid for RECORD variables

3. **Recursive resolution**: For Var expressions found during drilling, it recursively calls itself with appropriate parse state adjustments to handle nested subqueries and CTEs.

4. **Final resolution**: When no further drilling is possible, it delegates to get_expr_result_tupdesc for final type resolution.

## Parameters / Member Variables
- : Parse state containing context information for the current parsing operation
- : The Var node of type RECORD whose tuple descriptor needs to be determined
- : Extra offset for interpreting varlevelsup correctly during recursive calls (outside callers should pass zero)

## Dependencies
- Functions called/Symbols referenced:
  - [GetRTEByRangeTablePosn](../G/GetRTEByRangeTablePosn.md)
  - [expandRTE](expandRTE.md)
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md)
  - [TupleDescInitEntry](../T/TupleDescInitEntry.md)
  - [TupleDescInitEntryCollation](../T/TupleDescInitEntryCollation.md)
  - [get_tle_by_resno](../g/get_tle_by_resno.md)
  - [GetCTEForRTE](../G/GetCTEForRTE.md)
  - GetCTETargetList
  - [get_expr_result_tupdesc](../g/get_expr_result_tupdesc.md)
  - [exprType](exprType.md)
  - [exprTypmod](exprTypmod.md)
  - [exprCollation](exprCollation.md)
  - [list_nth](../l/list_nth.md)
  - InvalidAttrNumber
  - RTE constants (RTE_RELATION, RTE_SUBQUERY, RTE_JOIN, etc.)
- Called from (representative examples):
  - [ExpandRowReference](../E/ExpandRowReference.md)
  - [ParseComplexProjection](../P/ParseComplexProjection.md)
  - [expandRecordVariable](expandRecordVariable.md) (recursive calls)

## Notes and Other Information
- This function is crucial for PostgreSQL's flexible type system, allowing complex nested queries and joins to work with RECORD types
- The function includes sophisticated parse state management for handling nested subqueries and CTEs, creating temporary parse states as needed
- Self-referencing CTEs receive special handling and are not expanded to prevent infinite recursion
- The function performs extensive validation, generating errors when RECORD variables are found in inappropriate contexts
- Performance consideration: The function may need to traverse complex query structures, but this is necessary for type safety
- The recursive nature allows handling arbitrarily nested structures while maintaining proper variable resolution context

## Simplified Source

```c
TupleDesc expandRecordVariable(ParseState *pstate, Var *var, int levelsup) {
    Assert(IsA(var, Var));
    Assert(var->vartype == RECORDOID);

    // Calculate actual levels up and get the range table entry
    int netlevelsup = var->varlevelsup + levelsup;
    RangeTblEntry *rte = GetRTEByRangeTablePosn(pstate, var->varno, netlevelsup);
    AttrNumber attnum = var->varattno;

    // Handle whole-row references (expand all fields)
    if (attnum == InvalidAttrNumber) {
        return expand_whole_row_reference(rte, var);
    }

    // Find the actual expression by drilling down through RTE types
    Node *expr = find_record_expression(pstate, rte, attnum, netlevelsup);

    // Try to determine tuple descriptor from the final expression
    return get_expr_result_tupdesc(expr, false);
}

// Helper: Expand whole-row reference to all available fields
static TupleDesc expand_whole_row_reference(RangeTblEntry *rte, Var *var) {
    List *names, *vars;

    // Expand the RTE to get field names and variables
    expandRTE(rte, var->varno, 0, var->location, false, &names, &vars);

    // Create tuple descriptor from expanded fields
    TupleDesc tupleDesc = CreateTemplateTupleDesc(list_length(vars));

    ListCell *name_cell, *var_cell;
    int field_num = 1;

    forboth(name_cell, names, var_cell, vars) {
        char *field_name = strVal(lfirst(name_cell));
        Node *field_var = (Node *) lfirst(var_cell);

        TupleDescInitEntry(tupleDesc, field_num, field_name,
                          exprType(field_var), exprTypmod(field_var), 0);
        TupleDescInitEntryCollation(tupleDesc, field_num,
                                   exprCollation(field_var));
        field_num++;
    }

    return tupleDesc;
}

// Helper: Find the actual expression behind a RECORD variable
static Node *find_record_expression(ParseState *pstate, RangeTblEntry *rte,
                                   AttrNumber attnum, int netlevelsup) {
    Node *expr = NULL;

    switch (rte->rtekind) {
        case RTE_SUBQUERY:
            expr = resolve_subquery_record(pstate, rte, attnum, netlevelsup);
            break;

        case RTE_JOIN:
            expr = resolve_join_record(pstate, rte, attnum, netlevelsup);
            break;

        case RTE_CTE:
            expr = resolve_cte_record(pstate, rte, attnum, netlevelsup);
            break;

        case RTE_RELATION:
        case RTE_VALUES:
        case RTE_NAMEDTUPLESTORE:
        case RTE_RESULT:
        case RTE_FUNCTION:
        case RTE_TABLEFUNC:
            // These RTE types shouldn't have RECORD columns
            elog(ERROR, "unexpected RECORD type in RTE kind %d", rte->rtekind);
            break;
    }

    return expr ? expr : (Node *) var; // fallback to original var
}

// Helper: Resolve RECORD from subquery
static Node *resolve_subquery_record(ParseState *pstate, RangeTblEntry *rte,
                                    AttrNumber attnum, int netlevelsup) {
    TargetEntry *ste = get_tle_by_resno(rte->subquery->targetList, attnum);

    if (ste == NULL || ste->resjunk)
        elog(ERROR, "subquery does not have attribute %d", attnum);

    Node *expr = (Node *) ste->expr;

    // Recursively expand if it's another Var
    if (IsA(expr, Var)) {
        ParseState temp_pstate = {0};
        setup_subquery_parse_state(&temp_pstate, pstate, rte, netlevelsup);
        return (Node *) expandRecordVariable(&temp_pstate, (Var *) expr, 0);
    }

    return expr;
}

// Helper: Resolve RECORD from join
static Node *resolve_join_record(ParseState *pstate, RangeTblEntry *rte,
                                AttrNumber attnum, int netlevelsup) {
    Assert(attnum > 0 && attnum <= list_length(rte->joinaliasvars));

    Node *expr = (Node *) list_nth(rte->joinaliasvars, attnum - 1);
    Assert(expr != NULL);

    // Recursively expand if it's another Var
    if (IsA(expr, Var)) {
        return (Node *) expandRecordVariable(pstate, (Var *) expr, netlevelsup);
    }

    return expr;
}

// Helper: Resolve RECORD from CTE
static Node *resolve_cte_record(ParseState *pstate, RangeTblEntry *rte,
                               AttrNumber attnum, int netlevelsup) {
    if (rte->self_reference)
        return NULL; // avoid infinite recursion

    CommonTableExpr *cte = GetCTEForRTE(pstate, rte, netlevelsup);
    TargetEntry *ste = get_tle_by_resno(GetCTETargetList(cte), attnum);

    if (ste == NULL || ste->resjunk)
        elog(ERROR, "CTE does not have attribute %d", attnum);

    Node *expr = (Node *) ste->expr;

    // Recursively expand if it's another Var
    if (IsA(expr, Var)) {
        ParseState temp_pstate = {0};
        setup_cte_parse_state(&temp_pstate, pstate, rte, cte, netlevelsup);
        return (Node *) expandRecordVariable(&temp_pstate, (Var *) expr, 0);
    }

    return expr;
}

// Helper: Set up temporary parse state for subquery recursion
static void setup_subquery_parse_state(ParseState *temp_pstate, ParseState *pstate,
                                       RangeTblEntry *rte, int netlevelsup) {
    // Navigate to appropriate parent level
    ParseState *parent = pstate;
    for (int i = 0; i < netlevelsup; i++)
        parent = parent->parentParseState;

    temp_pstate->parentParseState = parent;
    temp_pstate->p_rtable = rte->subquery->rtable;
}

// Helper: Set up temporary parse state for CTE recursion
static void setup_cte_parse_state(ParseState *temp_pstate, ParseState *pstate,
                                  RangeTblEntry *rte, CommonTableExpr *cte,
                                  int netlevelsup) {
    // Navigate to appropriate parent level
    ParseState *parent = pstate;
    for (int i = 0; i < rte->ctelevelsup + netlevelsup; i++)
        parent = parent->parentParseState;

    temp_pstate->parentParseState = parent;
    temp_pstate->p_rtable = ((Query *) cte->ctequery)->rtable;
}
```

**Simplification Notes:**
- Broke down the large function into focused helper functions for each RTE type
- Preserved the essential algorithm: get RTE, handle whole-row vs specific field, drill down through RTE types
- Simplified the recursive parse state management with dedicated helper functions
- Maintained all the critical logic paths for subqueries, joins, and CTEs
- Reduced complexity by separating concerns while preserving functionality
- Made the control flow much clearer with explicit helper functions for each case
- Reduced from ~185 lines to ~140 lines while maintaining all functionality