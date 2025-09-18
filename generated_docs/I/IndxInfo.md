# IndxInfo

## Location
[src/bin/pg_dump/pg_dump.h:425-426](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L425-L426)

## Overview
IndxInfo is a structure used by pg_dump to represent database indexes during the schema dumping process, containing comprehensive metadata about index properties and relationships.

## Definition


## Detailed Description
IndxInfo is a comprehensive data structure in PostgreSQL's pg_dump utility that represents database indexes during the dumping process. It extends the base DumpableObject to capture all necessary information about an index, including its definition, storage properties, statistics, and relationships to tables and constraints.

The structure handles both regular indexes and partitioned indexes, storing information about parent-child relationships and partition attachments. It also manages the connection between indexes and their associated constraints (such as primary key or unique constraints), enabling pg_dump to maintain proper dependencies during the restore process.

This structure is crucial for preserving index definitions, including complex properties like clustering information, replication identity settings, and custom storage options that affect index behavior and performance.

## Parameters / Member Variables
- : Base DumpableObject containing common metadata like catalog ID, dump ID, name, namespace, and dependencies
- : Pointer to the TableInfo structure representing the table this index belongs to
- : Complete SQL definition of the index (CREATE INDEX statement)
- : Name of the tablespace where the index is stored, NULL if default
- : Index-specific options specified with WITH clause during creation
- : String representation of column numbers that have extended statistics
- : String representation of statistic values for the specified columns
- : Number of key attributes in the index (excluding included columns)
- : Total number of attributes in the index (including both key and included columns)
- : Array of attribute numbers for all index columns (both key and non-key attributes)
- : Boolean indicating if this index is used for table clustering
- : Boolean indicating if this index serves as replica identity
- : Boolean indicating if NULL values are considered equal for uniqueness
- : OID of the parent index if this is a partition index, InvalidOid otherwise
- : List of partition attach objects if this is a partitioned index
- : DumpId of the associated constraint object (for indexes backing constraints)

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (inherited base structure)
  - [TableInfo](../T/TableInfo.md) (referenced via indextable pointer)
  - [SimplePtrList](../S/SimplePtrList.md) (for partition attachments)
  - DumpId (for constraint associations)
- Called from (representative examples):
  - [getIndexes](../g/getIndexes.md) (creates and populates IndxInfo structures)
  - [dumpIndex](../d/dumpIndex.md) (processes index dumps)
  - [dumpConstraint](../d/dumpConstraint.md) (handles constraint-backed indexes)
  - [findIndexByOid](../f/findIndexByOid.md) (index lookup operations)
  - [flagInhIndexes](../f/flagInhIndexes.md) (inheritance processing)

## Notes and Other Information
- [IndxInfo](IndxInfo.md) structures are created during the schema discovery phase by getIndexes()
- The indexdef field contains the complete CREATE INDEX statement ready for execution
- Partition indexes maintain parent-child relationships through parentidx and partattaches members
- Indexes backing constraints (PRIMARY KEY, UNIQUE) have their constraint relationship tracked via indexconstraint
- The indkeys array layout changed over PostgreSQL versions to accommodate included columns
- Statistics-related fields (indstatcols, indstatvals) support extended statistics features
- This structure is essential for maintaining index dependencies during parallel restore operations