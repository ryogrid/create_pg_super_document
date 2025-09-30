# clause_selectivity

## Location
[src/backend/optimizer/path/clausesel.c:667-683](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/clausesel.c#L667-L683)

## Overview
Computes the selectivity of a general boolean expression clause, serving as the primary interface for selectivity estimation in PostgreSQL's query optimizer.

## Definition
```c
Selectivity
clause_selectivity(PlannerInfo *root,
                   Node *clause,
                   int varRelid,
                   JoinType jointype,
                   SpecialJoinInfo *sjinfo)
```

## Detailed Description
This function is a wrapper around `clause_selectivity_ext()` that provides the standard interface for computing clause selectivity estimates. It accepts either a RestrictInfo or a plain expression clause and attempts to cache selectivity values when RestrictInfo structures are used for potential reuse.

The function supports two primary modes of operation:
1. **Restriction mode** (varRelid != 0): Variables from the specified relation are treated as variables, while others are treated as constants with unknown values. This is used for estimating selectivity of join clauses being used as restriction clauses in nestloop inner scans.
2. **General mode** (varRelid == 0): All variables are treated as variables, appropriate for ordinary join and restriction clauses.

The function handles various join contexts through the `sjinfo` parameter, supporting both special joins (with full context from root->join_info_list) and transient INNER joins with limited context information.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global optimizer information and statistics
- `clause`: Node representing the boolean expression clause (RestrictInfo or plain expression)
- `varRelid`: Relation ID for restriction mode (0 for general mode)
- `jointype`: Type of join operation (JOIN_INNER for non-join clauses)
- `sjinfo`: SpecialJoinInfo providing join context (NULL for non-join clauses)

## Dependencies
- Functions called/Symbols referenced:
  - [clause_selectivity_ext](clause_selectivity_ext.md) (with use_extended_stats = true)
- Called from (representative examples):
  - [approx_tuple_count](../a/approx_tuple_count.md)
  - [get_foreign_key_join_selectivity](../g/get_foreign_key_join_selectivity.md)
  - [consider_new_or_clause](consider_new_or_clause.md)
  - [booltestsel](../b/booltestsel.md)

## Notes and Other Information
- This is a thin wrapper that always calls clause_selectivity_ext with extended statistics enabled
- [RestrictInfo](../R/RestrictInfo.md) structures are preferred input for caching selectivity estimates
- The function documentation includes detailed guidance on parameter usage for different scenarios
- Supports variant joins where jointype may differ from sjinfo->jointype (LHS/RHS switched or input unique-ified)
- When using varRelid != 0, it's typically appropriate to set jointype=JOIN_INNER and sjinfo=NULL

## Simplified Source

```c
Selectivity clause_selectivity(PlannerInfo *root,
                              Node *clause,
                              int varRelid,
                              JoinType jointype,
                              SpecialJoinInfo *sjinfo)
{
    // Simply delegate to extended version with statistics enabled
    return clause_selectivity_ext(root, clause, varRelid,
                                 jointype, sjinfo, true);
}
```