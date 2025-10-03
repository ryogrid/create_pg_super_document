# dependencies_clauselist_selectivity

## Location
[src/backend/statistics/dependencies.c:1370-1829](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/dependencies.c#L1370-L1829)

## Overview
Main entry point for estimating the selectivity of a clause list using functional dependency statistics, implementing a comprehensive algorithm that finds applicable dependencies and combines selectivities while avoiding double-counting.

## Definition

```c
Selectivity
dependencies_clauselist_selectivity(PlannerInfo *root,
									List *clauses,
									int varRelid,
									JoinType jointype,
									SpecialJoinInfo *sjinfo,
									RelOptInfo *rel,
									Bitmapset **estimatedclauses)
```
## Detailed Description
This function serves as the primary interface for applying functional dependency statistics during selectivity estimation. It implements a sophisticated multi-stage algorithm:

1. **Compatibility Analysis**: Processes each clause to determine compatibility with functional dependencies, handling both simple column references and complex expressions
2. **Expression Management**: Assigns negative attribute numbers to expressions for unified processing with regular attributes, maintaining consistency across statistics objects
3. **Statistics Loading**: Loads and filters relevant dependency statistics, matching them against available clauses and expressions
4. **Attribute Remapping**: Normalizes attribute numbers across different statistics objects to enable unified processing
5. **Dependency Selection**: Iteratively finds the strongest applicable dependencies using 
6. **Selectivity Application**: Applies selected dependencies using 

The algorithm handles complex scenarios including:
- Mixed attribute and expression dependencies
- Multiple overlapping statistics objects
- Dependency chains (a→b→c)
- Expression deduplication and consistent numbering

The mathematical foundation uses the formula:


Applied recursively for multi-attribute dependencies.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing query planning context
- `*clauses`: List of WHERE clauses to estimate selectivity for
- `varRelid`: Relation ID for the target relation
- `jointype`: Type of join operation being planned
- `*sjinfo`: Special join information structure
- `*rel`: RelOptInfo structure containing relation statistics
- `**estimatedclauses`: Input/output bitmapset tracking which clauses have been estimated
## Dependencies
- Functions called/Symbols referenced:
  - [has_stats_of_kind](../h/has_stats_of_kind.md)
  - [dependency_is_compatible_clause](dependency_is_compatible_clause.md)
  - [dependency_is_compatible_expression](dependency_is_compatible_expression.md)
  - planner_rt_fetch
  - [statext_dependencies_load](../s/statext_dependencies_load.md)
  - [find_strongest_dependency](../f/find_strongest_dependency.md)
  - [clauselist_apply_dependencies](../c/clauselist_apply_dependencies.md)
  - [bms_membership](../b/bms_membership.md), bms_add_member, bms_del_member, bms_free
- Types used:
  - JoinType
  - [SpecialJoinInfo](../S/SpecialJoinInfo.md)
  - [MVDependencies](../M/MVDependencies.md)
  - MVDependency
  - [StatisticExtInfo](../S/StatisticExtInfo.md)
  - STATS_EXT_DEPENDENCIES
- Called from (representative examples):
  - [statext_clauselist_selectivity](../s/statext_clauselist_selectivity.md)

## Notes and Other Information
- Returns 1.0 (no selectivity change) when no applicable dependencies are found
- Handles expression statistics by assigning them negative attribute numbers as pseudo-system attributes
- Performs extensive validation and normalization to ensure consistent attribute numbering across statistics objects
- Manages memory carefully with proper cleanup of allocated structures
- The algorithm prioritizes stronger/wider dependencies first to maximize accuracy
- Supports inheritance-aware statistics matching via  checking
- Efficiently filters out incompatible dependencies early to minimize processing overhead
- The function is the main orchestrator that coordinates all other dependency-related functions in the selectivity estimation pipeline

## Simplified Source

```c
Selectivity
dependencies_clauselist_selectivity(PlannerInfo *root, List *clauses, int varRelid,
                                   JoinType jointype, SpecialJoinInfo *sjinfo,
                                   RelOptInfo *rel, Bitmapset **estimatedclauses)
{
    Selectivity s1 = 1.0;
    Bitmapset *clauses_attnums = NULL;
    AttrNumber *list_attnums;
    MVDependencies **func_dependencies;
    int nfunc_dependencies = 0;
    MVDependency **dependencies;
    int ndependencies = 0;
    AttrNumber attnum_offset;
    RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);

    // Early exit if no dependency statistics available
    if (!has_stats_of_kind(rel->statlist, STATS_EXT_DEPENDENCIES)) {
        return 1.0;
    }

    list_attnums = (AttrNumber *) palloc(sizeof(AttrNumber) * list_length(clauses));
    Node **unique_exprs = (Node **) palloc(sizeof(Node *) * list_length(clauses));
    int unique_exprs_cnt = 0;

    // Process clauses to extract compatible attributes and expressions
    int listidx = 0;
    ListCell *l;
    foreach(l, clauses) {
        Node *clause = (Node *) lfirst(l);
        AttrNumber attnum;
        Node *expr = NULL;

        list_attnums[listidx] = InvalidAttrNumber;

        if (!bms_is_member(listidx, *estimatedclauses)) {
            if (dependency_is_compatible_clause(clause, rel->relid, &attnum)) {
                list_attnums[listidx] = attnum;
            } else if (dependency_is_compatible_expression(clause, rel->relid,
                                                          rel->statlist, &expr)) {
                // Handle expressions with negative attribute numbers
                attnum = InvalidAttrNumber;
                for (int i = 0; i < unique_exprs_cnt; i++) {
                    if (equal(unique_exprs[i], expr)) {
                        attnum = -(i + 1);
                        break;
                    }
                }
                if (attnum == InvalidAttrNumber) {
                    unique_exprs[unique_exprs_cnt++] = expr;
                    attnum = (-unique_exprs_cnt);
                }
                list_attnums[listidx] = attnum;
            }
        }
        listidx++;
    }

    // Calculate attribute offset for expressions
    attnum_offset = (unique_exprs_cnt > 0) ? (unique_exprs_cnt + 1) : 0;

    // Build clauses_attnums bitmap with offset attributes
    for (int i = 0; i < list_length(clauses); i++) {
        if (list_attnums[i] != InvalidAttrNumber) {
            AttrNumber attnum = list_attnums[i] + attnum_offset;
            list_attnums[i] = attnum;
            clauses_attnums = bms_add_member(clauses_attnums, attnum);
        }
    }

    // Need at least two distinct attributes/expressions
    if (bms_membership(clauses_attnums) != BMS_MULTIPLE) {
        // Cleanup and return
        bms_free(clauses_attnums);
        pfree(list_attnums);
        return 1.0;
    }

    // Load applicable functional dependencies
    func_dependencies = (MVDependencies **) palloc(sizeof(MVDependencies *) *
                                                   list_length(rel->statlist));
    foreach(l, rel->statlist) {
        StatisticExtInfo *stat = (StatisticExtInfo *) lfirst(l);

        if (stat->kind != STATS_EXT_DEPENDENCIES || stat->inherit != rte->inh) {
            continue;
        }

        // Count matching attributes and expressions
        int nmatched = 0;
        int k = -1;
        while ((k = bms_next_member(stat->keys, k)) >= 0) {
            AttrNumber attnum = (AttrNumber) k;
            if (AttrNumberIsForUserDefinedAttr(attnum)) {
                attnum += attnum_offset;
                if (bms_is_member(attnum, clauses_attnums)) {
                    nmatched++;
                }
            }
        }

        // Check expression matches
        int nexprs = 0;
        for (int i = 0; i < unique_exprs_cnt; i++) {
            ListCell *lc;
            foreach(lc, stat->exprs) {
                if (equal((Node *) lfirst(lc), unique_exprs[i])) {
                    nexprs++;
                }
            }
        }

        if (nmatched + nexprs >= 2) {
            MVDependencies *deps = statext_dependencies_load(stat->statOid, rte->inh);
            // Apply attribute remapping for expressions...
            if (deps->ndeps > 0) {
                func_dependencies[nfunc_dependencies++] = deps;
            }
        }
    }

    if (nfunc_dependencies == 0) {
        // Cleanup and return
        pfree(func_dependencies);
        bms_free(clauses_attnums);
        pfree(list_attnums);
        pfree(unique_exprs);
        return 1.0;
    }

    // Find and apply strongest dependencies iteratively
    dependencies = (MVDependency **) palloc(sizeof(MVDependency *) *
                                           /* total dependency count */);
    while (true) {
        MVDependency *dependency = find_strongest_dependency(func_dependencies,
                                                            nfunc_dependencies,
                                                            clauses_attnums);
        if (!dependency) {
            break;
        }

        dependencies[ndependencies++] = dependency;

        // Remove implied attribute for next iteration
        AttrNumber attnum = dependency->attributes[dependency->nattributes - 1];
        clauses_attnums = bms_del_member(clauses_attnums, attnum);
    }

    // Apply dependencies to compute final selectivity
    if (ndependencies != 0) {
        s1 = clauselist_apply_dependencies(root, clauses, varRelid, jointype,
                                          sjinfo, dependencies, ndependencies,
                                          list_attnums, estimatedclauses);
    }

    // Cleanup
    for (int i = 0; i < nfunc_dependencies; i++) {
        pfree(func_dependencies[i]);
    }
    pfree(dependencies);
    pfree(func_dependencies);
    bms_free(clauses_attnums);
    pfree(list_attnums);
    pfree(unique_exprs);

    return s1;
}
```