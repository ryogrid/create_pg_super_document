# TableInfo

## Location
src/bin/pg_dump/pg_dump.h: 379 - 380

## Overview
TableInfo represents table, view, sequence, and other relation objects in PostgreSQL's pg_dump utility, storing comprehensive metadata required for dumping and restoring database relations.

## Definition


## Detailed Description
TableInfo is the most comprehensive structure in pg_dump, representing all types of relations including tables, views, materialized views, sequences, and foreign tables. It contains extensive metadata about the relation's structure, attributes, constraints, indexes, triggers, and various PostgreSQL-specific properties. The structure is designed with a two-phase approach: basic information is collected for all relations, while detailed attribute and dependency information is gathered only for "interesting" relations that need to be dumped or are parents of dumpable objects.

## Parameters / Member Variables
### Basic Object Information
- : Base DumpableObject containing common dump metadata
- : DumpableAcl containing access control list information
- : Owner role name of the relation

### Relation Properties
- : Relation kind ('r' for table, 'v' for view, 'S' for sequence, etc.)
- : Persistence level ('p' for permanent, 't' for temporary, 'u' for unlogged)
- : Whether the relation is populated (mainly for materialized views)
- : Replica identity setting for logical replication
- : Tablespace where the relation is stored
- : Storage parameters specified with WITH clause
- : WITH CHECK OPTION setting for views
- : Storage parameters for the associated TOAST table

### Relation Features
- : Whether the relation has any indexes
- : Whether the relation has any rules
- : Whether the relation has any triggers  
- : Whether any columns have non-default access privileges
- : Whether row-level security is enabled
- : Whether row-level security is forced for table owner
- : Whether the table has OID column (deprecated feature)

### MVCC and Storage Information
- : Transaction ID freeze point for the relation
- : Minimum multixact ID for the relation
- : Object ID of associated TOAST table
- : Transaction ID freeze point for TOAST table
- : Minimum multixact ID for TOAST table
- : Size of relation in disk pages
- : Size of TOAST table in disk pages

### Type and Constraint Information  
- : Number of CHECK constraints
- : OID of the relation's composite type
- : OID of underlying type for typed tables
- : OID of foreign server for foreign tables

### Sequence Information
- : OID of table that owns this sequence
- : Column number that owns this sequence
- : Whether this is an identity sequence

### Processing Flags
- : Whether detailed information should be collected
- : Whether view definition should be postponed
- : Whether materialized view should be postponed to post-data
- : Whether this table is a partition
- : Whether this is an unsafe partitioned table

### Inheritance Information
- : Number of immediate parent tables
- : Array of pointers to parent TableInfo structures

### Detailed Attribute Information (populated only for interesting tables)
- : Number of attributes in the relation
- : Array of attribute names
- : Array of formatted attribute type names
- : Array of statistics collection targets
- : Array of storage strategies for attributes
- : Array of type-level storage strategies
- : Array indicating dropped attributes
- : Array of identity column settings
- : Array of generated column settings
- : Array of attribute lengths (for binary upgrades)
- : Array of attribute alignments (for binary upgrades)
- : Array indicating locally defined attributes
- : Array of per-attribute options
- : Array of attribute collations
- : Array of attribute compression methods
- : Array of foreign data wrapper options per attribute
- : Array of missing values for attributes
- : Array of NOT NULL constraints
- : Array indicating inherited NOT NULL constraints
- : Array of pointers to AttrDefInfo for DEFAULT expressions
- : Pointer to CHECK constraint information
- : Whether table has GENERATED ALWAYS AS IDENTITY columns
- : Access method name

### Associated Objects (for dumpable tables only)
- : Number of indexes on the table
- : Array of associated index information
- : Pointer to TableDataInfo for data dumping
- : Number of triggers on the table  
- : Array of trigger information

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
  - DumpableAcl
  - [AttrDefInfo](../A/AttrDefInfo.md)
  - [ConstraintInfo](../C/ConstraintInfo.md)
  - [IndxInfo](../I/IndxInfo.md)
  - [TableDataInfo](TableDataInfo.md)
  - [TriggerInfo](TriggerInfo.md)
  - [getTables](../g/getTables.md)
  - [getTableAttrs](../g/getTableAttrs.md)
  - [selectDumpableTable](../s/selectDumpableTable.md)
- Called from (representative examples):
  - [getTables](../g/getTables.md) (src/bin/pg_dump/pg_dump.c:6813)
  - [findTableByOid](../f/findTableByOid.md) (src/bin/pg_dump/common.c:861)
  - [dumpTable](../d/dumpTable.md) (src/bin/pg_dump/pg_dump.c:15717)
  - [dumpTableSchema](../d/dumpTableSchema.md) (src/bin/pg_dump/pg_dump.c:15946)

## Notes and Other Information
- [TableInfo](TableInfo.md) is the central data structure in pg_dump for representing database relations
- Memory allocation follows a two-phase approach: basic info for all tables, detailed info only for interesting ones
- The structure handles all PostgreSQL relation types (tables, views, sequences, foreign tables, etc.)
- Inheritance relationships are tracked through the parents array and numParents field
- Partitioning information is stored in ispartition and related fields
- The interesting flag determines whether expensive operations like attribute analysis are performed
- Used extensively throughout pg_dump for dependency resolution, object dumping, and cross-reference resolution