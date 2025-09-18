# PartitionDirectoryData

## Location
[src/backend/partitioning/partdesc.c:35-40](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partdesc.c#L35-L40)

## Overview
PartitionDirectoryData is a structure that maintains a cache of partition descriptors for efficient lookup and management of partitioned table metadata in PostgreSQL.

## Definition
```c
typedef struct PartitionDirectoryData
{
    MemoryContext pdir_mcxt;
    HTAB       *pdir_hash;
    bool        omit_detached;
} PartitionDirectoryData;
```

## Detailed Description
PartitionDirectoryData serves as a directory (cache) for partition descriptors, providing a centralized location to store and retrieve PartitionDesc objects for partitioned relations. The structure is designed to ensure that the same PartitionDesc is returned for a given relation OID throughout the lifetime of the directory, maintaining consistency even in the face of concurrent DDL operations.

The directory uses a hash table to map relation OIDs to PartitionDirectoryEntry objects, which contain the actual partition descriptors along with relation references. This caching mechanism improves performance by avoiding repeated construction of partition descriptors and ensures consistency within a single transaction or operation context.

## Parameters / Member Variables
- `pdir_mcxt`: Memory context in which the partition directory and its hash table are allocated, allowing for clean memory management and cleanup
- `pdir_hash`: Hash table that maps relation OIDs to PartitionDirectoryEntry objects, providing efficient lookup of cached partition descriptors
- `omit_detached`: Boolean flag indicating whether detached partitions should be omitted from partition descriptors retrieved through this directory

## Dependencies
- Functions called/Symbols referenced:
  - [HTAB](../H/HTAB.md) (hash table type from utils/hsearch.h)
- Called from (representative examples):
  - [CreatePartitionDirectory](../C/CreatePartitionDirectory.md)
  - [PartitionDirectory](PartitionDirectory.md) (as typedef pointer)

## Notes and Other Information
- The structure is typically accessed through the PartitionDirectory typedef, which is a pointer to PartitionDirectoryData
- Memory management is handled through the associated memory context, ensuring proper cleanup when the directory is destroyed
- The omit_detached flag affects all partition descriptors retrieved through this directory, providing consistent behavior for snapshot visibility of detached partitions
- Used internally by the partition descriptor caching system to maintain consistency across multiple lookups within the same operational context