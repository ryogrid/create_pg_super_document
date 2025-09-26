# add_part_relids

## Location
[src/backend/partitioning/partprune.c:392-437](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partprune.c#L392-L437)

## Overview
Adds new partition relation IDs to a list of Bitmapsets, organizing them by topmost parent partitioned relations.

## Definition
```c
static List *add_part_relids(List *allpartrelids, Bitmapset *partrelids)
```

## Detailed Description
This function manages a collection of partition hierarchies by adding newly identified partition relation IDs to the appropriate existing hierarchy or creating a new hierarchy entry. Each element in the allpartrelids list represents one topmost parent partitioned relation and contains a Bitmapset of RT (Range Table) indexes for that parent and its relevant non-leaf child partitions.

The function identifies the topmost parent by finding the lowest set bit in the Bitmapset, which works because parent partitions always have lower RT indexes than their children in the range table construction. When a matching topmost parent is found, the new partition IDs are merged into the existing hierarchy. If no match is found, a new partition hierarchy is added to the list.

This organization is essential for efficient partition pruning as it groups related partitions together while maintaining the hierarchical structure needed for pruning decisions.

## Parameters / Member Variables
- `allpartrelids`: List of Bitmapsets, each representing a partition hierarchy with one topmost parent
- `partrelids`: Bitmapset containing RT indexes of a parent partitioned relation and possibly some non-leaf children

## Dependencies
- Functions called/Symbols referenced:
  - [bms_next_member](../b/bms_next_member.md)
  - [bms_add_members](../b/bms_add_members.md)
  - [lappend](../l/lappend.md)
- Called from (representative examples):
  - [make_partition_pruneinfo](../m/make_partition_pruneinfo.md)

## Notes and Other Information
- Uses the property that parent partitions have lower RT indexes than children to identify topmost parents
- Only includes partitioned tables that are parents of scan-level relations in the subpaths
- Topmost parents are restricted to not be higher than the parentrel associated with the append path
- Handles cases where parentrel itself may be a child partitioned table
- The function is static and only used within the partition pruning subsystem