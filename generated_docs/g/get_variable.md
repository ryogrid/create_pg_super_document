# get_variable

## Location
[src/backend/utils/adt/ruleutils.c:7330-7602](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L7330-L7602)

## Overview
Displays a Var (variable reference) appropriately in the context of SQL rule decompilation, handling various special cases including whole-row variables, join aliases, and subplan references.

## Definition

```c
struct */
		if (attnum > colinfo->num_cols)
			elog(ERROR, "invalid attnum %d for relation \"%s\"",
				 attnum, rte->eref->aliasname);
```
## Detailed Description
This function is a core component of PostgreSQL's rule decompilation system that converts internal Var nodes back into readable SQL text. It handles the complex task of determining how to display variable references, taking into account nesting levels, join contexts, inheritance hierarchies, and various special cases.

The function performs several key operations:
1. Resolves the appropriate nesting depth using varlevelsup and levelsup parameters
2. Chooses between syntactic and semantic referents based on context
3. Handles special variable numbers (OUTER_VAR, INNER_VAR, INDEX_VAR) by delegating to resolve_special_varno
4. Maps child variables to parent relations when dealing with inheritance
5. Handles resjunk elements in subqueries by drilling down to subplan expressions
6. Processes unnamed join aliases by recursively expanding alias variables
7. Determines whether table prefixes are needed to avoid ambiguity
8. Formats the final output with appropriate quoting and type casting

## Parameters / Member Variables
- : The Var node to be displayed, containing variable number, attribute number, and type information
- : Additional nesting level offset to interpret varlevelsup relative to a context above the current one
- : Flag indicating if this Var appears at the top level of a SELECT targetlist, requiring special whole-row handling
- : Deparse context containing namespace information, buffer for output, and various formatting options

## Dependencies
- Functions called/Symbols referenced:
  - [list_nth](../l/list_nth.md)
  - rt_fetch
  - [bms_is_member](../b/bms_is_member.md)
  - deparse_columns_fetch
  - [resolve_special_varno](../r/resolve_special_varno.md)
  - [get_special_variable](get_special_variable.md)
  - [get_tle_by_resno](get_tle_by_resno.md)
  - [push_child_plan](../p/push_child_plan.md)/pop_child_plan
  - [get_rule_expr](get_rule_expr.md)
  - [get_rte_attribute_name](get_rte_attribute_name.md)
  - [quote_identifier](../q/quote_identifier.md)
  - [format_type_with_typemod](../f/format_type_with_typemod.md)
- Called from (representative examples):
  - [get_target_list](get_target_list.md)
  - [get_rule_sortgroupclause](get_rule_sortgroupclause.md)
  - [get_rule_expr](get_rule_expr.md)
  - [get_rule_expr_toplevel](get_rule_expr_toplevel.md)

## Notes and Other Information
- Returns the attribute name of the Var, or NULL if the Var has no attname (whole-row Vars or subplan references)
- Uses a "dirty hack" for top-level whole-row Vars, printing "tab.*::typename" instead of "tab.*" to prevent unwanted expansion
- Handles inheritance mapping by walking up the AppendRelInfo chain to find appropriate parent relations
- Contains recursive call to itself when processing join alias variables
- Includes special logic for ORDER BY clauses to add table prefixes when needed to avoid ambiguity with SELECT list items
- Critical for maintaining SQL standard compliance and readability in rule decompilation

## Simplified Source

```c
static char *get_variable(Var *var, int levelsup, bool istoplevel, deparse_context *context)
{
    StringInfo buf = context->buf;
    RangeTblEntry *rte;
    AttrNumber attnum;
    int netlevelsup;
    deparse_namespace *dpns;
    int varno;
    AttrNumber varattno;
    deparse_columns *colinfo;
    char *refname;
    char *attname;
    bool need_prefix;

    // Find appropriate nesting depth
    netlevelsup = var->varlevelsup + levelsup;
    if (netlevelsup >= list_length(context->namespaces))
        elog(ERROR, "bogus varlevelsup: %d offset %d", var->varlevelsup, levelsup);
    dpns = (deparse_namespace *) list_nth(context->namespaces, netlevelsup);

    // Choose between syntactic and semantic referent
    if (var->varnosyn > 0 && dpns->plan == NULL) {
        varno = var->varnosyn;
        varattno = var->varattnosyn;
    } else {
        varno = var->varno;
        varattno = var->varattno;
    }

    // Handle normal range table entries
    if (varno >= 1 && varno <= list_length(dpns->rtable)) {
        // Map child vars to parent relations if needed (inheritance)
        if (context->appendparents && dpns->appendrels) {
            int pvarno = varno;
            AttrNumber pvarattno = varattno;
            AppendRelInfo *appinfo = dpns->appendrels[pvarno];
            bool found = false;

            // Walk up inheritance hierarchy
            while (appinfo && rt_fetch(appinfo->parent_relid, dpns->rtable)->rtekind == RTE_RELATION) {
                found = false;
                if (pvarattno > 0) {  // system columns stay as-is
                    if (pvarattno > appinfo->num_child_cols)
                        break;
                    pvarattno = appinfo->parent_colnos[pvarattno - 1];
                    if (pvarattno == 0)
                        break;  // Var is local to child
                }
                pvarno = appinfo->parent_relid;
                found = true;
                appinfo = dpns->appendrels[pvarno];
            }

            if (found && bms_is_member(pvarno, context->appendparents)) {
                varno = pvarno;
                varattno = pvarattno;
            }
        }

        rte = rt_fetch(varno, dpns->rtable);
        refname = (char *) list_nth(dpns->rtable_names, varno - 1);
        colinfo = deparse_columns_fetch(varno, dpns);
        attnum = varattno;
    } else {
        // Handle special variable numbers (OUTER_VAR, INNER_VAR, etc.)
        resolve_special_varno((Node *) var, context, get_special_variable, NULL);
        return NULL;
    }

    // Handle resjunk elements in subqueries
    if ((rte->rtekind == RTE_SUBQUERY || rte->rtekind == RTE_CTE) &&
        attnum > list_length(rte->eref->colnames) && dpns->inner_plan) {
        TargetEntry *tle;
        deparse_namespace save_dpns;

        tle = get_tle_by_resno(dpns->inner_tlist, attnum);
        if (!tle)
            elog(ERROR, "invalid attnum %d for relation \"%s\"", attnum, rte->eref->aliasname);

        push_child_plan(dpns, dpns->inner_plan, &save_dpns);

        // Add parentheses for non-Var expressions
        if (!IsA(tle->expr, Var))
            appendStringInfoChar(buf, '(');
        get_rule_expr((Node *) tle->expr, context, true);
        if (!IsA(tle->expr, Var))
            appendStringInfoChar(buf, ')');

        pop_child_plan(dpns, &save_dpns);
        return NULL;
    }

    // Handle unnamed joins
    if (rte->rtekind == RTE_JOIN && rte->alias == NULL) {
        if (rte->joinaliasvars == NIL)
            elog(ERROR, "cannot decompile join alias var in plan tree");
        if (attnum > 0) {
            Var *aliasvar = (Var *) list_nth(rte->joinaliasvars, attnum - 1);
            if (aliasvar && IsA(aliasvar, Var)) {
                return get_variable(aliasvar, var->varlevelsup + levelsup,
                                  istoplevel, context);
            }
        }
        refname = NULL;  // Unnamed join has no refname
    }

    // Get attribute name
    if (attnum == InvalidAttrNumber)
        attname = NULL;
    else if (attnum > 0) {
        if (attnum > colinfo->num_cols)
            elog(ERROR, "invalid attnum %d for relation \"%s\"", attnum, rte->eref->aliasname);
        attname = colinfo->colnames[attnum - 1];
        if (attname == NULL)
            attname = "?dropped?column?";  // Handle dropped columns
    } else {
        attname = get_rte_attribute_name(rte, attnum);  // System column
    }

    need_prefix = (context->varprefix || attname == NULL);

    // Check if we need prefix in ORDER BY to avoid ambiguity
    if (context->varInOrderBy && !context->inGroupBy && !need_prefix) {
        int colno = 0;
        foreach_node(TargetEntry, tle, context->targetList) {
            char *colname;
            if (tle->resjunk)
                continue;
            colno++;

            if (context->resultDesc && colno <= context->resultDesc->natts)
                colname = NameStr(TupleDescAttr(context->resultDesc, colno - 1)->attname);
            else
                colname = tle->resname;

            if (colname && strcmp(colname, attname) == 0 && !equal(var, tle->expr)) {
                need_prefix = true;
                break;
            }
        }
    }

    // Output the variable
    if (refname && need_prefix) {
        appendStringInfoString(buf, quote_identifier(refname));
        appendStringInfoChar(buf, '.');
    }
    if (attname)
        appendStringInfoString(buf, quote_identifier(attname));
    else {
        appendStringInfoChar(buf, '*');
        if (istoplevel)  // Special handling for top-level whole-row vars
            appendStringInfo(buf, "::%s",
                           format_type_with_typemod(var->vartype, var->vartypmod));
    }

    return attname;
}
```