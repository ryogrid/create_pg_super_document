# find_appinfos_by_relids

## Location
[src/backend/optimizer/util/appendinfo.c:733-788](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/appendinfo.c#L733-L788)

## Overview
Finds AppendRelInfo structures for base relations listed in a given relids bitmapset, commonly used in join planning to identify child relations in partitioned or inheritance hierarchies.

## Definition
AppendRelInfo **find_appinfos_by_relids(PlannerInfo *root, Relids relids, int *nappinfos)

## Detailed Description
This function searches through the planner's append_rel_array to locate AppendRelInfo structures corresponding to the relation IDs specified in the relids bitmapset. It is typically called with a join relation's relids, which may include both base relations and outer-join RT indexes. The function silently ignores outer-join indexes and only processes base relations that have corresponding AppendRelInfo entries.

The function allocates an array to hold the found AppendRelInfo pointers and iterates through each member of the relids bitmapset. For each relation ID, it checks if there's a corresponding entry in root->append_rel_array. If no entry exists, it verifies whether the ID corresponds to an outer-join index (which is ignored) or a base relation without an append_rel_array entry (which triggers an error).

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and append_rel_array
- : Bitmapset of relation IDs to search for (typically from a join relation)
- : Output parameter set to the number of AppendRelInfo structures found

## Dependencies
- Functions called/Symbols referenced:
  - [bms_num_members](../b/bms_num_members.md): Gets the number of members in the relids bitmapset
  - [bms_next_member](../b/bms_next_member.md): Iterates through bitmapset members
  - [find_base_rel_ignore_join](find_base_rel_ignore_join.md): Checks if a relation ID corresponds to a base relation
  - [palloc](../p/palloc.md): Allocates memory for the result array
  - elog: Reports errors for missing append_rel_array entries

- Called from (representative examples):
  - [try_partitionwise_join](../t/try_partitionwise_join.md): For partitionwise join planning
  - [build_child_join_sjinfo](../b/build_child_join_sjinfo.md): When building child join information
  - [adjust_appendrel_attrs_multilevel](../a/adjust_appendrel_attrs_multilevel.md): For multi-level attribute adjustment
  - [create_partitionwise_grouping_paths](../c/create_partitionwise_grouping_paths.md): In grouping path creation

## Notes and Other Information
- The function allocates memory for an array sized to accommodate all members of the relids bitmapset, but the actual number of AppendRelInfo structures returned may be smaller due to ignored outer-join indexes
- The returned array can be freed by the caller using pfree
- An error is raised if a base relation ID is found without a corresponding append_rel_array entry, indicating an inconsistent planner state
- This function is essential for partitioned table and inheritance hierarchy processing in the PostgreSQL query planner