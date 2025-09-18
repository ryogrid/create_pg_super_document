# PartitionDirectoryEntry

## Location
src/backend/partitioning/partdesc.c: 42 - 47

## Overview
PartitionDirectoryEntry is a structure that represents an individual entry in a partition directory hash table, caching a partition descriptor along with its associated relation.

## Definition
```c
typedef struct PartitionDirectoryEntry
{
    Oid         reloid;
    Relation    rel;
    PartitionDesc pd;
} PartitionDirectoryEntry;
```

## Detailed Description
PartitionDirectoryEntry serves as a cache entry within a PartitionDirectory's hash table, storing the essential information needed to maintain and access partition metadata for a specific partitioned relation. Each entry associates a relation OID with its corresponding Relation object and PartitionDesc, ensuring that the same partition descriptor is consistently returned for subsequent lookups.

The structure maintains a reference to the relation to prevent the underlying PartitionDesc from being destroyed while the directory entry exists. This reference counting mechanism ensures memory safety and consistency of the cached partition descriptors throughout the lifetime of the partition directory.

## Parameters / Member Variables
- `reloid`: The object identifier (OID) of the partitioned relation, used as the hash key for lookups in the partition directory
- `rel`: Reference to the Relation object for the partitioned table, maintained to prevent premature destruction of the associated PartitionDesc
- `pd`: The cached PartitionDesc object containing the partition metadata, including information about all child partitions

## Dependencies
- Functions called/Symbols referenced:
  - PartitionDesc (partition descriptor type)
- Called from (representative examples):
  - CreatePartitionDirectory (for hash table entry size calculation)
  - PartitionDirectoryLookup (for hash table lookups and entry creation)
  - DestroyPartitionDirectory (for cleanup and reference count management)

## Notes and Other Information
- Used exclusively as hash table entries within PartitionDirectoryData's pdir_hash table
- The reloid field serves as the hash key, allowing efficient O(1) lookups of partition descriptors by relation OID
- Reference counting on the rel field ensures that the PartitionDesc remains valid for the lifetime of the directory entry
- Entries are automatically created on first lookup and persist until the partition directory is destroyed
- The structure enables consistent partition descriptor access across multiple operations within the same transaction or execution context