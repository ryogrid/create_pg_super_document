# _indexAttachInfo

## Location
[src/bin/pg_dump/pg_dump.h:427-431](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L427-L431)

## Overview
The  structure represents information needed to attach partition indexes to their parent partitioned indexes during PostgreSQL dump/restore operations.

## Definition

```c
typedef struct _indexAttachInfo
{
	DumpableObject dobj;
	IndxInfo   *parentIdx;		/* link to index on partitioned table */
	IndxInfo   *partitionIdx;	/* link to index on partition */
} IndexAttachInfo;
```
## Detailed Description
The  structure is used by pg_dump to manage the relationship between indexes on partitioned tables and their corresponding indexes on individual partitions. When pg_dump encounters a partitioned table with indexes, it needs to recreate not only the indexes themselves but also the logical attachment relationships between parent and child indexes. This structure stores the information needed to generate the appropriate  commands during restore.

The structure is created during the dependency analysis phase of pg_dump and is processed during the dump phase to output the necessary SQL commands for recreating the index attachment relationships.

## Parameters / Member Variables
- `dobj`: Base DumpableObject structure containing common dump object metadata (object type, catalog ID, dump ID, name, namespace)
- `*parentIdx`: Pointer to the IndxInfo structure representing the index on the parent partitioned table
- `*partitionIdx`: Pointer to the IndxInfo structure representing the corresponding index on the partition table
## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (base structure)
  - [IndxInfo](../I/IndxInfo.md) (for parent and partition index information)
  
- Called from (representative examples):
  - [flagInhIndexes](../f/flagInhIndexes.md)() (creates IndexAttachInfo objects)
  - [dumpIndexAttach](../d/dumpIndexAttach.md)() (processes the attachment during dump)

## Notes and Other Information
- The structure is allocated using pg_malloc_object() in flagInhIndexes()
- Objects of this type have objType set to DO_INDEX_ATTACH
- The dependency relationships are explicitly managed since they don't match entries in pg_depend
- Only created for partitioned tables that have inherited indexes
- Used exclusively within the pg_dump utility for handling partitioned table index relationships