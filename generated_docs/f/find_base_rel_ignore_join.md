# find_base_rel_ignore_join

## Location
[src/backend/optimizer/util/relnode.c:454-485](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/relnode.c#L454-L485)

## Overview
Finds a base or otherrel relation entry that must already exist, but returns NULL instead of raising an error if the relid references an outer join.

## Definition
RelOptInfo *find_base_rel_ignore_join(PlannerInfo *root, int relid)

## Detailed Description
This function is a variant of find_base_rel that provides more graceful handling of outer join references. While find_base_rel would raise an error when encountering an outer join relid, this function returns NULL instead. This behavior makes it convenient for callers that must deal with relid sets that include both base relations and outer joins.

The function first performs bounds checking using an unsigned comparison to prevent negative array access. If the relid is within bounds, it attempts to retrieve the relation from the simple_rel_array. If found, it returns the relation immediately. If not found, the function performs additional validation by checking if the relid corresponds to an outer join in the range table entry array. If it's confirmed to be an outer join (RTE_JOIN with jointype != JOIN_INNER), the function returns NULL. Otherwise, it raises an error for invalid relids.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and relation arrays
- : Integer identifier of the relation to find

## Dependencies
- Functions called/Symbols referenced:
  - JOIN_INNER (constant comparison)
  - RTE_JOIN (constant comparison)
  - elog (error logging)
- Called from (representative examples):
  - [create_lateral_join_info](../c/create_lateral_join_info.md)
  - [find_appinfos_by_relids](find_appinfos_by_relids.md)
  - [add_join_clause_to_rels](../a/add_join_clause_to_rels.md)
  - [remove_join_clause_from_rels](../r/remove_join_clause_from_rels.md)

## Notes and Other Information
- Uses unsigned comparison for bounds checking to prevent negative array access
- Includes debugging validation to verify that missing relations are actually outer joins
- Returns NULL for outer joins but raises ERROR for truly invalid relids
- Located in src/backend/optimizer/util/relnode.c:454-485

## Simplified Source

```c
RelOptInfo *
find_base_rel_ignore_join(PlannerInfo *root, int relid)
{
    // Check bounds using unsigned comparison to prevent negative access
    if ((uint32) relid < (uint32) root->simple_rel_array_size)
    {
        RelOptInfo *rel = root->simple_rel_array[relid];

        // Return relation if found
        if (rel)
            return rel;

        // Check if this is an outer join (return NULL if so)
        RangeTblEntry *rte = root->simple_rte_array[relid];
        if (rte && rte->rtekind == RTE_JOIN && rte->jointype != JOIN_INNER)
            return NULL;
    }

    // Error for invalid relids
    elog(ERROR, "no relation entry for relid %d", relid);
    return NULL;
}
```