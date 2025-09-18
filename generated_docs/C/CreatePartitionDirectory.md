# CreatePartitionDirectory

## Location
src/backend/partitioning/partdesc.c: 423 - 455

## Overview
Creates a new partition directory object that provides efficient hash-based lookup for partition descriptors.

## Definition


## Detailed Description
CreatePartitionDirectory initializes a new partition directory structure that serves as a cache for partition descriptors. The directory uses a hash table to enable fast lookups of partition information by relation OID. The function allocates the directory structure and its hash table in the specified memory context, allowing for proper memory management and cleanup.

The partition directory is particularly useful in scenarios where multiple partition lookups are expected, as it avoids repeated calls to RelationGetPartitionDesc for the same relations. The omit_detached parameter controls whether the directory should exclude detached partitions when building partition descriptors.

## Parameters / Member Variables
- : Memory context in which to allocate the partition directory and its hash table
- : Boolean flag indicating whether detached partitions should be omitted from partition descriptors

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextSwitchTo
  - palloc
  - hash_create
  - PartitionDirectoryData (structure type)
  - PartitionDirectoryEntry (hash table entry type)
  - HASHCTL (hash table control structure)
  - HASH_ELEM, HASH_BLOBS, HASH_CONTEXT (hash table flags)

- Called from:
  - ExecInitPartitionDispatchInfo
  - CreatePartitionPruneState  
  - set_relation_partition_info

## Notes and Other Information
- The function creates a hash table with 256 initial buckets for partition directory entries
- Memory context switching ensures all allocations are done in the specified context
- The hash table uses OID as the key and PartitionDirectoryEntry as the entry type
- The directory maintains the omit_detached setting for consistent behavior across lookups
- Hash table flags include HASH_ELEM (for fixed-size elements), HASH_BLOBS (for simple key comparison), and HASH_CONTEXT (for memory context allocation)