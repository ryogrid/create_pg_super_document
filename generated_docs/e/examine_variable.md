# examine_variable

## Location
[src/backend/utils/adt/selfuncs.c:5025-5350](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L5025-L5350)

## Overview
Analyzes an expression tree to extract statistical information and fill a VariableStatData structure with details about the variable, its type, uniqueness, and associated statistics.

## Definition

```c
void
examine_variable(PlannerInfo *root, Node *node, int varRelid,
				 VariableStatData *vardata)
```
## Detailed Description
This is a central function in PostgreSQL's selectivity estimation system that performs comprehensive analysis of expressions to gather statistical information. The function handles various types of expressions, from simple column references (Vars) to complex expressions involving multiple relations.

The function follows a multi-layered approach: it first handles simple Var nodes as a fast path, then proceeds to analyze more complex expressions by determining their variable membership across relations. For expressions that reference columns from a single relation, it attempts to find matching statistics either from regular column statistics, expressional indexes, or extended statistics objects.

The function is particularly sophisticated in handling expressional indexes and extended statistics. When an expression matches an index expression, it retrieves statistics from the index and determines if the expression represents a unique value. For extended statistics, it searches through statistics objects that contain per-expression statistics and loads the appropriate statistical data.

The function also handles security considerations by checking whether the current user has permission to access the underlying table data, which affects whether certain statistical information can be used safely.

## Parameters / Member Variables
- : Pointer to PlannerInfo structure containing planner context and query information
- : The expression tree to be analyzed for statistical information
- : Relation ID for restriction context; when nonzero, only variables from this relation are considered as variables
- : Output parameter that gets filled with comprehensive information about the variable including statistics, type information, uniqueness, and access permissions

## Dependencies
- Functions called/Symbols referenced:
  - MemSet
  - [exprType](exprType.md)
  - [exprTypmod](exprTypmod.md)
  - IsA
  - [find_base_rel](../f/find_base_rel.md)
  - [find_join_rel](../f/find_join_rel.md)
  - [has_unique_index](../h/has_unique_index.md)
  - [examine_simple_variable](examine_simple_variable.md)
  - [pull_varnos](../p/pull_varnos.md)
  - [bms_difference](../b/bms_difference.md)
  - bms_is_empty
  - [bms_get_singleton_member](../b/bms_get_singleton_member.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [bms_overlap](../b/bms_overlap.md)
  - [bms_free](../b/bms_free.md)
  - [remove_nulling_relids](../r/remove_nulling_relids.md)
  - [list_head](../l/list_head.md)
  - [lnext](../l/lnext.md)
  - [equal](equal.md)
  - [SearchSysCache3](../S/SearchSysCache3.md)
  - HeapTupleIsValid
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [all_rows_selectable](../a/all_rows_selectable.md)
  - [statext_expressions_load](../s/statext_expressions_load.md)
  - [ReleaseDummy](../R/ReleaseDummy.md)
  - planner_rt_fetch
- Called from (representative examples):
  - [get_restriction_variable](../g/get_restriction_variable.md)
  - [get_join_variables](../g/get_join_variables.md)
  - [boolvarsel](../b/boolvarsel.md)
  - [booltestsel](../b/booltestsel.md)
  - [nulltestsel](../n/nulltestsel.md)
  - [estimate_array_length](estimate_array_length.md)
  - [mergejoinscansel](../m/mergejoinscansel.md)
  - [estimate_num_groups](estimate_num_groups.md)
  - [estimate_hash_bucket_stats](estimate_hash_bucket_stats.md)
  - [scalararraysel_containment](../s/scalararraysel_containment.md)

## Notes and Other Information
- The function initializes vardata with MemSet to ensure no dangling pointers are returned
- For simple Var nodes, it provides a fast path that directly retrieves column statistics and uniqueness information
- The function strips binary-compatible relabeling to work with the underlying expression structure
- It differentiates between base relations and join relations when analyzing variable membership
- For expressional indexes, it searches through all index expressions to find matches and retrieves corresponding statistics
- Extended statistics support allows the function to find statistics for complex expressions that aren't covered by regular column or index statistics
- Security checks ensure that statistical information is only used when the current user has appropriate table access permissions
- The caller is responsible for calling ReleaseVariableStats() to clean up any allocated statistical data
- The function handles inheritance hierarchies correctly by checking permissions on the appropriate parent relation

## Simplified Source

```c
void examine_variable(PlannerInfo *root, Node *node, int varRelid,
                     VariableStatData *vardata) {
    Node *basenode;
    Relids varnos, basevarnos;
    RelOptInfo *onerel;

    // Initialize vardata structure
    MemSet(vardata, 0, sizeof(VariableStatData));
    vardata->vartype = exprType(node);

    // Strip binary relabeling if present
    if (IsA(node, RelabelType))
        basenode = (Node *) ((RelabelType *) node)->arg;
    else
        basenode = node;

    // Fast path for simple Var nodes
    if (IsA(basenode, Var) &&
        (varRelid == 0 || varRelid == ((Var *) basenode)->varno)) {
        Var *var = (Var *) basenode;

        vardata->var = basenode;
        vardata->rel = find_base_rel(root, var->varno);
        vardata->atttype = var->vartype;
        vardata->atttypmod = var->vartypmod;
        vardata->isunique = has_unique_index(vardata->rel, var->varattno);

        examine_simple_variable(root, var, vardata);
        return;
    }

    // Handle more complex expressions
    varnos = pull_varnos(root, basenode);
    basevarnos = bms_difference(varnos, root->outer_join_rels);
    onerel = NULL;

    if (!bms_is_empty(basevarnos)) {
        int relid;

        // Check if expression involves single base relation
        if (bms_get_singleton_member(basevarnos, &relid)) {
            if (varRelid == 0 || varRelid == relid) {
                onerel = find_base_rel(root, relid);
                vardata->rel = onerel;
                node = basenode; // strip relabeling
            }
        } else {
            // Multiple relations involved
            if (varRelid == 0) {
                vardata->rel = find_join_rel(root, varnos);
                node = basenode;
            } else if (bms_is_member(varRelid, varnos)) {
                vardata->rel = find_base_rel(root, varRelid);
                node = basenode;
            }
        }
    }

    bms_free(basevarnos);

    vardata->var = node;
    vardata->atttype = exprType(node);
    vardata->atttypmod = exprTypmod(node);

    // Try to find statistics for single-relation expressions
    if (onerel) {
        // Strip nulling relids for better matching
        if (bms_overlap(varnos, root->outer_join_rels))
            node = remove_nulling_relids(node, root->outer_join_rels, NULL);

        // Search expressional indexes
        ListCell *ilist;
        foreach(ilist, onerel->indexlist) {
            IndexOptInfo *index = (IndexOptInfo *) lfirst(ilist);
            ListCell *indexpr_item = list_head(index->indexprs);

            if (!indexpr_item) continue;

            for (int pos = 0; pos < index->ncolumns; pos++) {
                if (index->indexkeys[pos] == 0) {
                    Node *indexkey = (Node *) lfirst(indexpr_item);
                    if (indexkey && IsA(indexkey, RelabelType))
                        indexkey = (Node *) ((RelabelType *) indexkey)->arg;

                    if (equal(node, indexkey)) {
                        // Check for uniqueness
                        if (index->unique && index->nkeycolumns == 1 && pos == 0 &&
                            (index->indpred == NIL || index->predOK))
                            vardata->isunique = true;

                        // Get statistics if available
                        if (index->indpred == NIL) {
                            vardata->statsTuple = SearchSysCache3(STATRELATTINH,
                                ObjectIdGetDatum(index->indexoid),
                                Int16GetDatum(pos + 1),
                                BoolGetDatum(false));
                            vardata->freefunc = ReleaseSysCache;

                            if (HeapTupleIsValid(vardata->statsTuple)) {
                                vardata->acl_ok = all_rows_selectable(root,
                                    index->rel->relid, NULL);
                            } else {
                                vardata->acl_ok = true;
                            }
                        }
                        if (vardata->statsTuple) break;
                    }
                    indexpr_item = lnext(index->indexprs, indexpr_item);
                }
            }
            if (vardata->statsTuple) break;
        }

        // Search extended statistics for expressions
        if (!vardata->statsTuple) {
            ListCell *slist;
            foreach(slist, onerel->statlist) {
                StatisticExtInfo *info = (StatisticExtInfo *) lfirst(slist);
                RangeTblEntry *rte = planner_rt_fetch(onerel->relid, root);

                if (info->kind != STATS_EXT_EXPRESSIONS || info->inherit != rte->inh)
                    continue;

                int pos = 0;
                ListCell *expr_item;
                foreach(expr_item, info->exprs) {
                    Node *expr = (Node *) lfirst(expr_item);
                    if (expr && IsA(expr, RelabelType))
                        expr = (Node *) ((RelabelType *) expr)->arg;

                    if (equal(node, expr)) {
                        vardata->statsTuple = statext_expressions_load(info->statOid,
                                                                      rte->inh, pos);
                        vardata->freefunc = ReleaseDummy;
                        vardata->acl_ok = all_rows_selectable(root, onerel->relid, NULL);
                        break;
                    }
                    pos++;
                }
                if (vardata->statsTuple) break;
            }
        }
    }

    bms_free(varnos);
}
```