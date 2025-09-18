# LogicalRepPartMapEntry

## Location
src/backend/replication/logical/relation.c: 51 - 55

## Overview
LogicalRepPartMapEntry is a structure used in PostgreSQL logical replication to map partitioned table partitions to their corresponding remote relations, maintaining separate attribute mappings for each partition.

## Definition
```c
typedef struct LogicalRepPartMapEntry
{
    Oid             partoid;        /* LogicalRepPartMap's key */
    LogicalRepRelMapEntry relmapentry;
} LogicalRepPartMapEntry;
```

## Detailed Description
LogicalRepPartMapEntry serves as a specialized mapping structure for handling partitioned tables in PostgreSQL logical replication. When a partitioned table is used as a replication target, replicated operations are performed on individual leaf partitions rather than the parent table. Each partition may have different attribute numbers compared to the parent table, requiring separate attribute mappings to be maintained for proper replication.

This structure extends the basic LogicalRepRelMapEntry functionality by associating it with a specific partition OID, enabling the logical replication system to maintain distinct mapping information for each partition while reusing the core relation mapping logic.

The structure is used as an entry in the LogicalRepPartMap hash table, which provides efficient lookup of partition-specific replication mappings during logical replication operations.

## Parameters / Member Variables
- `partoid`: The OID of the partition relation, serving as the hash table key for LogicalRepPartMap lookups
- `relmapentry`: An embedded LogicalRepRelMapEntry structure containing the complete relation mapping information including remote relation details, local relation mapping, attribute mappings, and synchronization state

## Dependencies
- Functions called/Symbols referenced:
  - [LogicalRepRelMapEntry](LogicalRepRelMapEntry.md) (embedded structure)
  - Oid (PostgreSQL object identifier type)

- Called from (representative examples):
  - logicalrep_partmap_invalidate_cb (partition map invalidation callback)
  - [logicalrep_partmap_reset_relmap](../l/logicalrep_partmap_reset_relmap.md) (partition map reset function)
  - [logicalrep_partmap_init](../l/logicalrep_partmap_init.md) (partition map initialization)
  - [logicalrep_partition_open](../l/logicalrep_partition_open.md) (partition opening function)

## Notes and Other Information
- This structure is specifically designed for partitioned table replication scenarios where individual partitions require separate attribute mapping management
- The structure is stored in the LogicalRepPartMap hash table with partoid as the key
- Memory allocation for partition map entries is managed through the LogicalRepPartMapContext memory context
- The embedded relmapentry provides all the standard relation mapping capabilities while being partition-specific
- This design allows PostgreSQL to efficiently handle complex partitioned table replication scenarios while maintaining clean separation between parent table and partition-specific mapping logic