# get_name_for_var_field

## Location
[src/backend/utils/adt/ruleutils.c:7732-8161](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L7732-L8161)

## Overview
Determines the field name of a specified field within an expression of composite type, handling complex cases including RECORD types, special variables, subqueries, and CTEs through recursive analysis.

## Definition

```c
static const char *
get_name_for_var_field(Var *var, int fieldno,
					   int levelsup, deparse_context *context)
```
## Detailed Description
This function handles the complex task of determining field names for composite type expressions during rule decompilation. It deals with several challenging scenarios:

1. **RowExpr handling**: For RowExpr nodes expanded from whole-row Vars, directly extracts column names from the attached colnames list.

2. **RECORD type resolution**: When encountering RECORD types (which can't exist in actual table columns), the function drills down through various expression types to find the ultimate source and infer the field name.

3. **Parameter resolution**: For Param nodes of type RECORD, uses find_param_referent to locate the actual referenced expression and recursively processes it.

4. **Special variable handling**: Manages OUTER_VAR, INNER_VAR, and INDEX_VAR references by traversing into subplans and recursively calling itself on the resolved expressions.

5. **Complex RTE types**: Handles various range table entry types including:
   - Subqueries: Examines target list entries and may recurse into sub-select queries
   - Joins: Follows joinaliasvars to find the actual source expression  
   - CTEs: Locates the referenced CTE and examines its target list
   - Relations/Values/etc.: Generally should not occur for RECORD types

6. **Plan tree compatibility**: Includes special handling for plan trees where some information (like subquery details) may not be available, falling back to generic names like "f1", "f2", etc.

The function maintains proper namespace context throughout recursive calls and handles inheritance mapping appropriately.

## Parameters / Member Variables
- `*var`: The expression (typically a Var) whose field name is needed
- `fieldno`: The 1-based field number within the composite type
- `levelsup`: Additional nesting level offset for interpreting varlevelsup
- `*context`: Deparse context containing namespace stack and other formatting state
## Dependencies
- Functions called/Symbols referenced:
  - [find_param_referent](../f/find_param_referent.md) (for PARAM resolution)
  - [get_expr_result_tupdesc](get_expr_result_tupdesc.md) (for final tuple descriptor extraction)
  - [get_tle_by_resno](get_tle_by_resno.md) (target list entry retrieval)
  - [push_child_plan](../p/push_child_plan.md)/pop_child_plan (context management)
  - [push_ancestor_plan](../p/push_ancestor_plan.md)/pop_ancestor_plan (parameter context handling)
  - [set_deparse_for_query](../s/set_deparse_for_query.md) (subquery namespace setup)
  - [get_rte_attribute_name](get_rte_attribute_name.md) (system column names)
  - GetCTETargetList (CTE target list access)
  - [get_name_for_var_field](get_name_for_var_field.md) (recursive self-calls)
- Called from (representative examples):
  - [get_rule_expr](get_rule_expr.md) (for FieldSelect expressions)
  - [get_name_for_var_field](get_name_for_var_field.md) (recursive calls)

## Notes and Other Information
- Returns const char* pointing to the field name, which may be allocated in various memory contexts depending on the resolution path
- The function is heavily recursive and includes stack depth protection through check_stack_depth calls in called functions
- Handles both parse tree and plan tree contexts, with different logic paths for each
- When field names cannot be determined (empty subqueries/CTEs), falls back to generic "fN" naming convention
- Critical for proper handling of composite types in complex queries involving joins, subqueries, and CTEs
- The logic parallels the parser's expandRecordVariable() function but operates in the reverse direction during decompilation
- Includes extensive error checking for bogus variable references and missing target list entries

## Simplified Source

```c
static const char *get_name_for_var_field(Var *var, int fieldno,
                                          int levelsup, deparse_context *context) {
    RangeTblEntry *rte;
    AttrNumber attnum;
    int netlevelsup;
    deparse_namespace *dpns;
    int varno;
    AttrNumber varattno;
    TupleDesc tupleDesc;
    Node *expr;

    // Handle RowExpr - extract column name directly
    if (IsA(var, RowExpr)) {
        RowExpr *r = (RowExpr *) var;
        if (fieldno > 0 && fieldno <= list_length(r->colnames))
            return strVal(list_nth(r->colnames, fieldno - 1));
    }

    // Handle Param of RECORD type - find what it refers to
    if (IsA(var, Param)) {
        Param *param = (Param *) var;
        ListCell *ancestor_cell;

        expr = find_param_referent(param, context, &dpns, &ancestor_cell);
        if (expr) {
            // Recurse with proper context
            deparse_namespace save_dpns;
            const char *result;

            push_ancestor_plan(dpns, ancestor_cell, &save_dpns);
            result = get_name_for_var_field((Var *) expr, fieldno, 0, context);
            pop_ancestor_plan(dpns, &save_dpns);
            return result;
        }
    }

    // For non-RECORD types, use standard tuple descriptor
    if (!IsA(var, Var) || var->vartype != RECORDOID) {
        tupleDesc = get_expr_result_tupdesc((Node *) var, false);
        Assert(fieldno >= 1 && fieldno <= tupleDesc->natts);
        return NameStr(TupleDescAttr(tupleDesc, fieldno - 1)->attname);
    }

    // Find proper nesting level and namespace
    netlevelsup = var->varlevelsup + levelsup;
    dpns = (deparse_namespace *) list_nth(context->namespaces, netlevelsup);

    // Use syntactic or semantic referent
    if (var->varnosyn > 0 && dpns->plan == NULL) {
        varno = var->varnosyn;
        varattno = var->varattnosyn;
    } else {
        varno = var->varno;
        varattno = var->varattno;
    }

    // Handle special variables (OUTER_VAR, INNER_VAR, INDEX_VAR)
    if (varno == OUTER_VAR && dpns->outer_tlist) {
        TargetEntry *tle = get_tle_by_resno(dpns->outer_tlist, varattno);
        if (!tle) elog(ERROR, "bogus varattno for OUTER_VAR var: %d", varattno);

        deparse_namespace save_dpns;
        push_child_plan(dpns, dpns->outer_plan, &save_dpns);
        const char *result = get_name_for_var_field((Var *) tle->expr, fieldno, levelsup, context);
        pop_child_plan(dpns, &save_dpns);
        return result;
    }

    // Similar handling for INNER_VAR and INDEX_VAR...

    // Handle regular range table entries
    if (varno >= 1 && varno <= list_length(dpns->rtable)) {
        rte = rt_fetch(varno, dpns->rtable);
        attnum = varattno;
    } else {
        elog(ERROR, "bogus varno: %d", varno);
        return NULL;
    }

    // Handle whole-row reference
    if (attnum == InvalidAttrNumber) {
        return get_rte_attribute_name(rte, fieldno);
    }

    // Process different RTE types (simplified logic)
    expr = (Node *) var; // default fallback

    switch (rte->rtekind) {
        case RTE_SUBQUERY:
            // Extract from subquery target list
            if (rte->subquery) {
                TargetEntry *ste = get_tle_by_resno(rte->subquery->targetList, attnum);
                if (ste && !ste->resjunk) {
                    expr = (Node *) ste->expr;
                    if (IsA(expr, Var)) {
                        // Recurse into subquery context
                        // (simplified - full implementation handles namespace setup)
                        return get_name_for_var_field((Var *) expr, fieldno, 0, context);
                    }
                }
            }
            break;

        case RTE_JOIN:
            // Follow join alias variable
            if (rte->joinaliasvars != NIL) {
                expr = (Node *) list_nth(rte->joinaliasvars, attnum - 1);
                if (IsA(expr, Var))
                    return get_name_for_var_field((Var *) expr, fieldno,
                                                  var->varlevelsup + levelsup, context);
            }
            break;

        case RTE_CTE:
            // Similar to subquery handling for CTEs
            // (simplified - full implementation handles CTE lookup)
            break;

        default:
            // Other types shouldn't have RECORD fields
            break;
    }

    // Final fallback - try to get tuple descriptor
    tupleDesc = get_expr_result_tupdesc(expr, false);
    Assert(fieldno >= 1 && fieldno <= tupleDesc->natts);
    return NameStr(TupleDescAttr(tupleDesc, fieldno - 1)->attname);
}
```