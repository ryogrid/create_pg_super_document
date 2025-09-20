# IndexAttachInfo

## Location
[src/bin/pg_dump/pg_dump.h:432-433](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L432-L433)

## Overview
IndexAttachInfo represents the relationship between a partitioned index and its partition indexes in PostgreSQL's pg_dump utility, managing index attachment operations during database dumping and restoration.

## Definition

```c
typedef struct _statsExtInfo
{
	DumpableObject dobj;
	const char *rolname;		/* owner */
	TableInfo  *stattable;		/* link to table the stats are for */
	int			stattarget;		/* statistics target */
} StatsExtInfo;
```
## Detailed Description
IndexAttachInfo is a specialized data structure in pg_dump that manages the relationship between partitioned indexes and their corresponding partition indexes. This structure is essential for PostgreSQL's table partitioning feature, where indexes on partitioned tables need to be properly associated with indexes on individual partitions.

When a partitioned table has an index, PostgreSQL automatically creates corresponding indexes on each partition. The IndexAttachInfo structure captures these relationships and ensures that during database restoration, the partition indexes are properly attached to their parent partitioned index using the ALTER INDEX ... ATTACH PARTITION command.

This structure is critical for maintaining the integrity of partitioned index hierarchies during dump and restore operations, ensuring that query planner optimizations and constraint enforcement work correctly across the partition hierarchy.

## Parameters / Member Variables
- `dobj`: Base DumpableObject containing common metadata like catalog ID, dump ID, name, namespace, and dependencies
- `parentIdx`: Pointer to the IndxInfo structure representing the index on the partitioned (parent) table
- `partitionIdx`: Pointer to the IndxInfo structure representing the corresponding index on the individual partition

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (inherited base structure)
  - [IndxInfo](IndxInfo.md) (referenced by both parentIdx and partitionIdx pointers)
- Called from (representative examples):
  - [flagInhIndexes](../f/flagInhIndexes.md) (processes index inheritance relationships)
  - [dumpIndexAttach](../d/dumpIndexAttach.md) (generates index attachment commands)
  - [addConstrChildIdxDeps](../a/addConstrChildIdxDeps.md) (manages constraint dependencies)
  - [dumpDumpableObject](../d/dumpDumpableObject.md) (generic dump processing)

## Notes and Other Information
- [IndexAttachInfo](IndexAttachInfo.md) objects are created during the schema discovery phase when pg_dump detects partitioned index relationships
- These structures ensure proper ordering during restore: partition indexes are created before being attached to parent indexes
- The structure helps maintain referential integrity between parent and child indexes in partitioned table hierarchies
- During restore, IndexAttachInfo generates ALTER INDEX ... ATTACH PARTITION commands to rebuild the index hierarchy
- This is particularly important for unique and primary key constraints that span partition boundaries
- The dependency system ensures parent indexes exist before attempting to attach partition indexes
- Part of PostgreSQL's declarative partitioning infrastructure introduced in version 10