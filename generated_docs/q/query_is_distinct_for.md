# query_is_distinct_for

## Location
[src/backend/optimizer/plan/analyzejoins.c:995-1143](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/analyzejoins.c#L995-L1143)

## Overview
Determines whether a query is guaranteed to never return duplicate rows for a specified set of output columns, considering various SQL constructs that can ensure distinctness.

## Definition
```c
bool query_is_distinct_for(Query *query, List *colnos, List *opids)
```

## Detailed Description
This function performs a comprehensive analysis to determine if the given query structure guarantees distinctness for the specified columns. It examines various SQL constructs that can provide uniqueness guarantees:

1. **DISTINCT clauses**: Explicit DISTINCT or DISTINCT ON ensures uniqueness if all DISTINCT columns appear in the target column set and operators are compatible
2. **GROUP BY clauses**: Grouping without grouping sets guarantees uniqueness for the grouped columns
3. **Grouping sets**: Special handling for empty grouping sets (single row result) vs. multiple sets
4. **Aggregation**: Queries with aggregates or HAVING but no GROUP BY produce at most one row
5. **Set operations**: UNION/INTERSECT/EXCEPT without ALL guarantee whole-row uniqueness

The function also handles edge cases like set-returning functions (SRFs) which can break distinctness guarantees by producing multiple rows from single inputs.

Operator compatibility is checked using `equality_ops_are_compatible()` to ensure that the upper-level equality operators would consider values distinct in the same way as the subquery's operators.

## Parameters / Member Variables
- `query`: Pointer to the not-yet-planned subquery to analyze  
- `colnos`: List of integer output column numbers (resno's) to check for distinctness
- `opids`: List of equality operator OIDs corresponding to each column for compatibility checking

## Dependencies
- Functions called/Symbols referenced:
  - [list_length](../l/list_length.md)
  - [get_sortgroupclause_tle](../g/get_sortgroupclause_tle.md)
  - [distinct_col_search](../d/distinct_col_search.md)  
  - [equality_ops_are_compatible](../e/equality_ops_are_compatible.md)
  - castNode
  - [list_head](../l/list_head.md)
  - [lnext](../l/lnext.md)
  - linitial
- Data structures used:
  - [SortGroupClause](../S/SortGroupClause.md)
  - [TargetEntry](../T/TargetEntry.md)
  - [GroupingSet](../G/GroupingSet.md)
  - [SetOperationStmt](../S/SetOperationStmt.md)
- Called from:
  - [rel_is_distinct_for](../r/rel_is_distinct_for.md) (src/backend/optimizer/plan/analyzejoins.c:939)
  - [create_unique_path](../c/create_unique_path.md) (src/backend/optimizer/util/pathnode.c:1762)

## Notes and Other Information
- Must be kept in sync with `query_supports_distinctness()` for optimization consistency
- Handles cross-type operators by checking btree/hash opfamily membership for compatibility
- SRFs in the target list break distinctness guarantees unless handled by DISTINCT
- Grouping sets with expressions are considered too complex and punt to false
- Set operations check all non-junk output columns for completeness
- Part of PostgreSQL's advanced query optimization for join elimination and unique path generation

## Simplified Source

```c
bool
query_is_distinct_for(Query *query, List *colnos, List *opids)
{
    ListCell *l;
    Oid opid;

    Assert(list_length(colnos) == list_length(opids));

    // Check DISTINCT clause
    if (query->distinctClause) {
        foreach(l, query->distinctClause) {
            SortGroupClause *sgc = (SortGroupClause *) lfirst(l);
            TargetEntry *tle = get_sortgroupclause_tle(sgc, query->targetList);

            opid = distinct_col_search(tle->resno, colnos, opids);
            if (!OidIsValid(opid) || !equality_ops_are_compatible(opid, sgc->eqop))
                break; // No match found
        }
        if (l == NULL) // All DISTINCT columns matched
            return true;
    }

    // SRFs break distinctness guarantees
    if (query->hasTargetSRFs)
        return false;

    // Check GROUP BY (without grouping sets)
    if (query->groupClause && !query->groupingSets) {
        foreach(l, query->groupClause) {
            SortGroupClause *sgc = (SortGroupClause *) lfirst(l);
            TargetEntry *tle = get_sortgroupclause_tle(sgc, query->targetList);

            opid = distinct_col_search(tle->resno, colnos, opids);
            if (!OidIsValid(opid) || !equality_ops_are_compatible(opid, sgc->eqop))
                break; // No match found
        }
        if (l == NULL) // All GROUP BY columns matched
            return true;
    }
    else if (query->groupingSets) {
        // Single empty grouping set = one row = unique
        if (list_length(query->groupingSets) == 1 &&
            ((GroupingSet *) linitial(query->groupingSets))->kind == GROUPING_SET_EMPTY)
            return true;
        else
            return false;
    }
    else {
        // Aggregates or HAVING without GROUP BY = at most one row
        if (query->hasAggs || query->havingQual)
            return true;
    }

    // Check set operations (UNION/INTERSECT/EXCEPT without ALL)
    if (query->setOperations) {
        SetOperationStmt *topop = castNode(SetOperationStmt, query->setOperations);
        if (!topop->all) {
            ListCell *lg = list_head(topop->groupClauses);
            foreach(l, query->targetList) {
                TargetEntry *tle = (TargetEntry *) lfirst(l);
                if (tle->resjunk)
                    continue;

                SortGroupClause *sgc = (SortGroupClause *) lfirst(lg);
                lg = lnext(topop->groupClauses, lg);

                opid = distinct_col_search(tle->resno, colnos, opids);
                if (!OidIsValid(opid) || !equality_ops_are_compatible(opid, sgc->eqop))
                    break;
            }
            if (l == NULL) // All columns matched
                return true;
        }
    }

    return false;
}
```