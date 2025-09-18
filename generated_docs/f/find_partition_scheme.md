# find_partition_scheme

## Location
src/backend/optimizer/util/plancat.c: 2449 - 2555

## Overview
Finds an existing PartitionScheme that matches a relation's partitioning characteristics, or creates a new one if no match is found, for use in partition-aware query planning.

## Definition


## Detailed Description
This static function implements a caching mechanism for partition schemes by searching through existing schemes in the PlannerInfo's part_schemes list and returning a match if found, or creating and caching a new scheme if no match exists.

The matching process compares multiple partitioning characteristics:
- Partitioning strategy (e.g., RANGE, HASH, LIST)
- Number of partition key attributes
- Operator family OIDs for each partition key
- Input type OIDs for each partition key  
- Collation OIDs for each partition key
- Type length and byval properties (verified by assertion when other properties match)
- Partition support function OIDs (verified by assertion)

When creating a new scheme, the function allocates memory and copies all relevant partitioning metadata from the relation's PartitionKey to ensure the scheme persists beyond the relation's lifecycle. The new scheme is added to the planner's part_schemes list for future reuse.

## Parameters / Member Variables
- : PlannerInfo structure containing the list of existing partition schemes
- : The partitioned relation whose partition scheme is needed

## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetPartitionKey](../R/RelationGetPartitionKey.md)
  - [palloc0](../p/palloc0.md)
  - [palloc](../p/palloc.md)
  - memcpy
  - [fmgr_info_copy](fmgr_info_copy.md)
  - lappend
  - [PartitionKey](../P/PartitionKey.md)
  - PartitionScheme
  - PartitionSchemeData
- Called from (representative examples):
  - [set_relation_partition_info](../s/set_relation_partition_info.md)

## Notes and Other Information
- This is a static function only used within plancat.c
- The function assumes the relation is partitioned and has a valid partition key
- Caching partition schemes improves performance when multiple relations share the same partitioning characteristics
- All partition scheme data is copied to ensure it survives beyond the relation cache entry's lifetime
- The scheme comparison is comprehensive, ensuring that only truly equivalent partitioning configurations are matched
- Memory for the partition scheme is allocated in the current memory context and persists for the duration of planning