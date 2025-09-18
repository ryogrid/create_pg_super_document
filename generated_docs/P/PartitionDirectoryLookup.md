# PartitionDirectoryLookup

## Location
[src/backend/partitioning/partdesc.c:456-483](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partdesc.c#L456-L483)

## Overview
Looks up and caches partition descriptors in a partition directory, ensuring consistent views of partition information across concurrent DDL operations.

## Definition


## Detailed Description
PartitionDirectoryLookup provides a cached lookup mechanism for partition descriptors within a partition directory. The primary purpose is to ensure consistency by returning the same PartitionDesc for a given relation OID throughout the lifetime of the partition directory, even in the face of concurrent DDL operations that might result in different catalog views.

The function uses a hash table lookup to find existing entries or creates new ones as needed. When a new entry is created, it increments the relation's reference count to ensure the underlying relation and its partition descriptor remain valid for the duration of the directory's existence.

This caching mechanism is particularly important for query execution and planning phases where multiple lookups of the same partition information may occur, and consistency of that information is crucial for correctness.

## Parameters / Member Variables
- : The partition directory to search in
- : The partitioned relation to look up

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetRelid
  - [hash_search](../h/hash_search.md)
  - HASH_ENTER (hash operation flag)
  - [RelationIncrementReferenceCount](../R/RelationIncrementReferenceCount.md)
  - [RelationGetPartitionDesc](../R/RelationGetPartitionDesc.md)
  - [PartitionDirectoryEntry](PartitionDirectoryEntry.md) (hash table entry type)

- Called from:
  - [ExecInitPartitionDispatchInfo](../E/ExecInitPartitionDispatchInfo.md)
  - [CreatePartitionPruneState](../C/CreatePartitionPruneState.md)
  - [expand_partitioned_rtentry](../e/expand_partitioned_rtentry.md)
  - [set_relation_partition_info](../s/set_relation_partition_info.md)

## Notes and Other Information
- Maintains reference counts on relations to prevent premature cleanup of partition descriptors
- Uses HASH_ENTER to create new entries if they don't exist
- Ensures the same PartitionDesc is returned for the same OID throughout the directory's lifetime
- The function assumes the relation is partitioned (PartitionDesc should not be NULL)
- Provides protection against inconsistent views during concurrent DDL operations
- The omit_detached setting from the directory is passed through to RelationGetPartitionDesc