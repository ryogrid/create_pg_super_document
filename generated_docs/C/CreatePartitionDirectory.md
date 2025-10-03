# CreatePartitionDirectory

## Location
[src/backend/partitioning/partdesc.c:423-455](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partdesc.c#L423-L455)

## Overview
Creates a new partition directory object that provides efficient hash-based lookup for partition descriptors.

## Definition

```c
PartitionDirectory
CreatePartitionDirectory(MemoryContext mcxt, bool omit_detached)
```
## Detailed Description
CreatePartitionDirectory initializes a new partition directory structure that serves as a cache for partition descriptors. The directory uses a hash table to enable fast lookups of partition information by relation OID. The function allocates the directory structure and its hash table in the specified memory context, allowing for proper memory management and cleanup.

The partition directory is particularly useful in scenarios where multiple partition lookups are expected, as it avoids repeated calls to RelationGetPartitionDesc for the same relations. The omit_detached parameter controls whether the directory should exclude detached partitions when building partition descriptors.

## Parameters / Member Variables
- `mcxt`: Memory context in which to allocate the partition directory and its hash table
- `omit_detached`: Boolean flag indicating whether detached partitions should be omitted from partition descriptors
## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [palloc](../p/palloc.md)
  - [hash_create](../h/hash_create.md)
  - [PartitionDirectoryData](../P/PartitionDirectoryData.md) (structure type)
  - [PartitionDirectoryEntry](../P/PartitionDirectoryEntry.md) (hash table entry type)
  - [HASHCTL](../H/HASHCTL.md) (hash table control structure)
  - HASH_ELEM, HASH_BLOBS, HASH_CONTEXT (hash table flags)

- Called from:
  - [ExecInitPartitionDispatchInfo](../E/ExecInitPartitionDispatchInfo.md)
  - [CreatePartitionPruneState](CreatePartitionPruneState.md)  
  - [set_relation_partition_info](../s/set_relation_partition_info.md)

## Notes and Other Information
- The function creates a hash table with 256 initial buckets for partition directory entries
- Memory context switching ensures all allocations are done in the specified context
- The hash table uses OID as the key and PartitionDirectoryEntry as the entry type
- The directory maintains the omit_detached setting for consistent behavior across lookups
- [Hash](../H/Hash.md) table flags include HASH_ELEM (for fixed-size elements), HASH_BLOBS (for simple key comparison), and HASH_CONTEXT (for memory context allocation)

## Simplified Source

```c
PartitionDirectory
CreatePartitionDirectory(MemoryContext mcxt, bool omit_detached)
{
    MemoryContext oldcontext = MemoryContextSwitchTo(mcxt);
    PartitionDirectory pdir;
    HASHCTL ctl;

    // Allocate partition directory structure
    pdir = palloc(sizeof(PartitionDirectoryData));
    pdir->pdir_mcxt = mcxt;
    pdir->omit_detached = omit_detached;

    // Configure hash table for partition lookups
    ctl.keysize = sizeof(Oid);  // Key: relation OID
    ctl.entrysize = sizeof(PartitionDirectoryEntry);
    ctl.hcxt = mcxt;

    // Create hash table with 256 initial buckets
    pdir->pdir_hash = hash_create("partition directory", 256, &ctl,
                                 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

    MemoryContextSwitchTo(oldcontext);
    return pdir;
}
```