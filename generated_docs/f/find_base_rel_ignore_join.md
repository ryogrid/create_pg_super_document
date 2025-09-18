# find_base_rel_ignore_join

## Location
src/backend/optimizer/util/relnode.c: 454 - 485

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
  - create_lateral_join_info
  - find_appinfos_by_relids
  - add_join_clause_to_rels
  - remove_join_clause_from_rels

## Notes and Other Information
- Uses unsigned comparison for bounds checking to prevent negative array access
- Includes debugging validation to verify that missing relations are actually outer joins
- Returns NULL for outer joins but raises ERROR for truly invalid relids
- Located in src/backend/optimizer/util/relnode.c:454-485