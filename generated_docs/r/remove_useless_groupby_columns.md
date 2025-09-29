# remove_useless_groupby_columns

## Location
[src/backend/optimizer/plan/planner.c:2717-2883](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L2717-L2883)

## Overview
Optimizes GROUP BY clauses by removing columns that are functionally dependent on other GROUP BY columns, specifically those made redundant by primary key constraints.

## Definition
```c
static void remove_useless_groupby_columns(PlannerInfo *root)
```

## Detailed Description
This function performs an important query optimization by eliminating redundant columns from GROUP BY clauses. The optimization is based on the mathematical principle that if a tables primary key columns are included in the GROUP BY, then all other columns from that table are functionally determined and need not be grouped explicitly.

**Algorithm Steps:**

1. **Initial Checks**: 
   - Requires at least 2 GROUP BY items
   - Skips optimization if grouping sets are present

2. **Column Analysis**:
   - Scans processed_groupClause to identify simple Var references
   - Builds bitmapsets mapping relation IDs to their grouped column numbers
   - Ignores non-Vars, outer query variables, and complex expressions

3. **Primary Key Detection**:
   - For each relation with multiple grouped columns
   - Retrieves primary key column set using get_primary_key_attnos()
   - Identifies inheritance parent tables (except partitioned tables) to avoid duplicate row issues

4. **Redundancy Identification**:
   - Determines if primary key columns are a proper subset of grouped columns
   - Marks surplus columns (those beyond primary key requirements)

5. **GROUP BY Reconstruction**:
   - Builds new GROUP BY clause excluding redundant columns
   - Preserves non-Var expressions and necessary columns

## Parameters
- `root`: PlannerInfo structure containing the querys GROUP BY information to optimize

## Dependencies
- Functions called/Symbols referenced:
  - [get_sortgroupclause_tle](../g/get_sortgroupclause_tle.md)
  - [get_primary_key_attnos](../g/get_primary_key_attnos.md)
  - [bms_add_member](../b/bms_add_member.md), bms_membership, bms_subset_compare, bms_difference, bms_is_member
  - [SortGroupClause](../S/SortGroupClause.md) node handling
  - FirstLowInvalidHeapAttributeNumber constant
- Called from:
  - [grouping_planner](../g/grouping_planner.md)
  - standard_qp_extra

## Notes and Other Information
- **Performance Benefits**: Reduces sorting overhead by eliminating unnecessary grouping columns
- **Compatibility**: Handles queries written for DBMSes that require all selected columns in GROUP BY
- **Plan Invalidation**: Automatically invalidated when primary key constraints change via relcache
- **Future Extensions**: Could potentially be extended to unique NOT NULL constraints
- **Limitation**: Currently only handles simple Var references, not complex expressions
- **Safety**: Carefully handles inheritance hierarchies and outer query variables
- Located in src/backend/optimizer/plan/planner.c:2717-2883

## Simplified Source

```c
static void remove_useless_groupby_columns(PlannerInfo *root)
{
    Query *parse = root->parse;
    Bitmapset **groupbyattnos;
    Bitmapset **surplusvars;
    ListCell *lc;
    int relid;

    // Early exit for simple cases
    if (list_length(root->processed_groupClause) < 2)
        return;

    if (parse->groupingSets)
        return;

    // Build bitmapsets of GROUP BY columns for each relation
    groupbyattnos = (Bitmapset **) palloc0(sizeof(Bitmapset *) *
                                          (list_length(parse->rtable) + 1));
    foreach(lc, root->processed_groupClause) {
        SortGroupClause *sgc = lfirst_node(SortGroupClause, lc);
        TargetEntry *tle = get_sortgroupclause_tle(sgc, parse->targetList);
        Var *var = (Var *) tle->expr;

        // Only process simple Vars from current query level
        if (!IsA(var, Var) || var->varlevelsup > 0)
            continue;

        relid = var->varno;
        groupbyattnos[relid] = bms_add_member(groupbyattnos[relid],
                                            var->varattno - FirstLowInvalidHeapAttributeNumber);
    }

    // Find relations where some GROUP BY columns can be removed
    surplusvars = NULL;
    relid = 0;
    foreach(lc, parse->rtable) {
        RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);
        Bitmapset *relattnos;
        Bitmapset *pkattnos;
        Oid constraintOid;

        relid++;

        // Only regular relations have primary keys
        if (rte->rtekind != RTE_RELATION)
            continue;

        // Skip inheritance parents (except partitioned tables)
        if (rte->inh && rte->relkind != RELKIND_PARTITIONED_TABLE)
            continue;

        // Need multiple GROUP BY columns for this relation
        relattnos = groupbyattnos[relid];
        if (bms_membership(relattnos) != BMS_MULTIPLE)
            continue;

        // Get primary key columns
        pkattnos = get_primary_key_attnos(rte->relid, false, &constraintOid);
        if (pkattnos == NULL)
            continue;

        // Check if primary key is subset of GROUP BY columns
        if (bms_subset_compare(pkattnos, relattnos) == BMS_SUBSET1) {
            // Initialize surplus array if needed
            if (surplusvars == NULL)
                surplusvars = (Bitmapset **) palloc0(sizeof(Bitmapset *) *
                                                   (list_length(parse->rtable) + 1));

            // Mark surplus columns for removal
            surplusvars[relid] = bms_difference(relattnos, pkattnos);
        }
    }

    // Rebuild GROUP BY clause without surplus columns
    if (surplusvars != NULL) {
        List *new_groupby = NIL;

        foreach(lc, root->processed_groupClause) {
            SortGroupClause *sgc = lfirst_node(SortGroupClause, lc);
            TargetEntry *tle = get_sortgroupclause_tle(sgc, parse->targetList);
            Var *var = (Var *) tle->expr;

            // Keep non-Vars, outer Vars, and non-surplus columns
            if (!IsA(var, Var) ||
                var->varlevelsup > 0 ||
                !bms_is_member(var->varattno - FirstLowInvalidHeapAttributeNumber,
                              surplusvars[var->varno]))
                new_groupby = lappend(new_groupby, sgc);
        }

        root->processed_groupClause = new_groupby;
    }
}
```