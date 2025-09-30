# examine_simple_variable

## Location
[src/backend/utils/adt/selfuncs.c:5351-5617](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L5351-L5617)

## Overview
Handles examination of a simple Var for the examine_variable function, recursively processing variables that reference subqueries or CTEs to extract statistical information.

## Definition

```c
static void
examine_simple_variable(PlannerInfo *root, Var *var,
						VariableStatData *vardata)
```
## Detailed Description
This function is responsible for populating the statistical information in a VariableStatData structure for a simple variable reference. It handles various types of table references including regular relations, subqueries, and Common Table Expressions (CTEs).

The function operates by:
1. First checking if a custom stats hook is available and letting it handle stats acquisition
2. For regular relations (RTE_RELATION), looking up column statistics in pg_statistic and checking user permissions
3. For subqueries and CTEs, recursively analyzing the underlying query structure to extract relevant statistics
4. Handling security considerations by respecting security barriers and access permissions

The function is designed to be recursive, allowing it to drill down through multiple layers of subqueries to find the ultimate source of statistical data.

## Parameters / Member Variables
- : PlannerInfo structure containing the current planning context and query information
- : The variable (column reference) being examined for statistical information
- : Output structure that will be populated with statistical data and metadata about the variable

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache3](../S/SearchSysCache3.md) (for pg_statistic lookup)
  - [all_rows_selectable](../a/all_rows_selectable.md) (security permission checking)
  - [bms_make_singleton](../b/bms_make_singleton.md) (bitmap set operations)
  - [find_base_rel](../f/find_base_rel.md) (relation lookup)
  - [get_tle_by_resno](../g/get_tle_by_resno.md) (target list entry retrieval)
  - [targetIsInSortList](../t/targetIsInSortList.md) (DISTINCT clause analysis)
  - [examine_simple_variable](examine_simple_variable.md) (recursive self-call)
- Called from (representative examples):
  - [examine_variable](examine_variable.md) (main entry point for variable statistics examination)

## Notes and Other Information
- The function respects security barriers and row-level security policies when determining whether to expose statistical information
- For subqueries with DISTINCT clauses, it can sometimes determine uniqueness even when other statistics are unavailable
- The function handles complex cases like CTE references that may span multiple query levels
- Security considerations prevent accessing statistics from security barrier views to avoid information leakage
- The acl_ok field in vardata is set based on whether the user has permission to see all rows, affecting which statistical functions can be used later

## Simplified Source

```c
static void
examine_simple_variable(PlannerInfo *root, Var *var, VariableStatData *vardata)
{
    RangeTblEntry *rte = root->simple_rte_array[var->varno];

    // Try custom stats hook first
    if (get_relation_stats_hook &&
        (*get_relation_stats_hook)(root, rte, var->varattno, vardata)) {
        // Hook handled stats acquisition
        if (HeapTupleIsValid(vardata->statsTuple) && !vardata->freefunc)
            elog(ERROR, "no function provided to release variable stats with");
    }
    else if (rte->rtekind == RTE_RELATION) {
        // Regular table - look up column statistics in pg_statistic
        vardata->statsTuple = SearchSysCache3(STATRELATTINH,
                                            ObjectIdGetDatum(rte->relid),
                                            Int16GetDatum(var->varattno),
                                            BoolGetDatum(rte->inh));
        vardata->freefunc = ReleaseSysCache;

        // Check user permissions for accessing statistics
        if (HeapTupleIsValid(vardata->statsTuple)) {
            vardata->acl_ok = all_rows_selectable(root, var->varno,
                                                bms_make_singleton(var->varattno - FirstLowInvalidHeapAttributeNumber));
        } else {
            vardata->acl_ok = true; // No stats to protect
        }
    }
    else if ((rte->rtekind == RTE_SUBQUERY && !rte->inh) ||
             (rte->rtekind == RTE_CTE && !rte->self_reference)) {
        // Subquery or CTE - try to analyze the underlying expression

        if (var->varattno == InvalidAttrNumber)
            return; // Can't handle whole-row vars

        PlannerInfo *subroot = NULL;

        if (rte->rtekind == RTE_SUBQUERY) {
            // Get subquery's planner info
            RelOptInfo *rel = find_base_rel(root, var->varno);
            subroot = rel->subroot;
        } else {
            // CTE case - find the referenced CTE's subroot
            PlannerInfo *cteroot = root;
            Index levelsup = rte->ctelevelsup;

            // Navigate to the appropriate query level
            while (levelsup-- > 0) {
                cteroot = cteroot->parent_root;
                if (!cteroot)
                    elog(ERROR, "bad levelsup for CTE \"%s\"", rte->ctename);
            }

            // Find CTE in the list and get its plan ID
            int ndx = 0;
            ListCell *lc;
            foreach(lc, cteroot->parse->cteList) {
                CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);
                if (strcmp(cte->ctename, rte->ctename) == 0)
                    break;
                ndx++;
            }

            if (lc != NULL && ndx < list_length(cteroot->cte_plan_ids)) {
                int plan_id = list_nth_int(cteroot->cte_plan_ids, ndx);
                if (plan_id > 0)
                    subroot = list_nth(root->glob->subroots, plan_id - 1);
            }
        }

        if (subroot == NULL)
            return; // Subquery not planned yet

        Query *subquery = subroot->parse;

        // Skip if subquery has operations that destroy column stats
        if (subquery->setOperations || subquery->groupClause || subquery->groupingSets)
            return;

        // Get the target entry that this Var references
        List *subtlist = subquery->returningList ? subquery->returningList : subquery->targetList;
        TargetEntry *ste = get_tle_by_resno(subtlist, var->varattno);
        if (ste == NULL || ste->resjunk)
            elog(ERROR, "subquery %s does not have attribute %d", rte->eref->aliasname, var->varattno);

        var = (Var *) ste->expr;

        // Handle DISTINCT clause
        if (subquery->distinctClause) {
            if (list_length(subquery->distinctClause) == 1 &&
                targetIsInSortList(ste, InvalidOid, subquery->distinctClause))
                vardata->isunique = true;
            return; // Can't get other stats with DISTINCT
        }

        // Respect security barriers
        if (rte->security_barrier)
            return;

        // Recursively examine the underlying variable
        if (var && IsA(var, Var) && var->varlevelsup == 0) {
            examine_simple_variable(subroot, var, vardata);
        }
    }
    // Other RTE types (FUNCTION, VALUES) - no stats available
}
```