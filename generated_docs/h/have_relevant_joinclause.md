# have_relevant_joinclause

## Location
[src/backend/optimizer/util/joininfo.c:39-97](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/joininfo.c#L39-L97)

## Overview
Detects whether there is a joinclause that involves two given relations, used by the PostgreSQL query optimizer to determine if two relations can be meaningfully joined.

## Definition
```c
bool have_relevant_joinclause(PlannerInfo *root, RelOptInfo *rel1, RelOptInfo *rel2)
```

## Detailed Description
This function determines if there exists a join clause that involves both of the specified relations. The function implements an important optimization principle: the join clause does not need to be immediately evaluable with only these two relations. This allows for more sophisticated join ordering decisions.

For example, in a query like `SELECT * FROM a, b, c WHERE a.x = (b.y + c.z)`, even though the join condition cannot be evaluated with just relations b and c, it may still be beneficial to join b and c first (via cross-join) and then use an inner index scan on a.x if relation a is much larger than the others.

The function uses a two-phase approach:
1. Scans the joininfo lists of both relations to find overlapping required_relids
2. Falls back to checking EquivalenceClass data structures for relationships not captured in joininfo lists

For efficiency, it scans the shorter of the two joininfo lists.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planning information
- `rel1`: First relation to check for join relevance  
- `rel2`: Second relation to check for join relevance

## Dependencies
- Functions called/Symbols referenced:
  - [bms_overlap](../b/bms_overlap.md)
  - [have_relevant_eclass_joinclause](have_relevant_eclass_joinclause.md)
- Called from (representative examples):
  - [desirable_join](../d/desirable_join.md)
  - [join_search_one_level](../j/join_search_one_level.md)
  - [make_rels_by_clause_joins](../m/make_rels_by_clause_joins.md)
  - [has_legal_joinclause](has_legal_joinclause.md)

## Notes and Other Information
- The function prioritizes performance by choosing to scan the shorter joininfo list
- The EquivalenceClass fallback check ensures comprehensive coverage of potential join relationships
- This function is crucial for the query optimizer's join ordering decisions
- Located in src/backend/optimizer/util/joininfo.c:39-97

## Simplified Source

```c
bool
have_relevant_joinclause(PlannerInfo *root, RelOptInfo *rel1, RelOptInfo *rel2)
{
    bool result = false;
    List *joininfo;
    Relids other_relids;
    ListCell *l;

    // Use the shorter joininfo list for efficiency
    if (list_length(rel1->joininfo) <= list_length(rel2->joininfo))
    {
        joininfo = rel1->joininfo;
        other_relids = rel2->relids;
    }
    else
    {
        joininfo = rel2->joininfo;
        other_relids = rel1->relids;
    }

    // Check joininfo list for overlapping required_relids
    foreach(l, joininfo)
    {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);

        if (bms_overlap(other_relids, rinfo->required_relids))
        {
            result = true;
            break;
        }
    }

    // Fallback: check EquivalenceClass relationships
    if (!result && rel1->has_eclass_joins && rel2->has_eclass_joins)
        result = have_relevant_eclass_joinclause(root, rel1, rel2);

    return result;
}
```