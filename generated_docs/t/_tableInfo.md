# _tableInfo

## Location
src/bin/pg_dump/pg_dump.h: 295 - 378

## Overview
The  structure is a comprehensive data structure used by pg_dump to store metadata information about database tables during the dump process.

## Definition


## Detailed Description
The  structure serves as the central repository for all metadata related to database tables in pg_dump. It contains comprehensive information about table properties, attributes, constraints, indexes, and relationships. The structure is organized into three logical sections: basic table information collected for all tables, detailed attribute information computed for interesting tables, and dump-specific data for tables that will be dumped.

## Parameters / Member Variables
### Basic Table Information
- : Base dumpable object information
- : Access control list information
- : Name of the table owner role
- : Relation kind (table, view, sequence, etc.)
- : Relation persistence (permanent, temporary, unlogged)
- : Whether the relation is populated
- : Replica identity setting
- : Tablespace name where the table resides
- : Storage options specified with WITH clause
- : WITH CHECK OPTION for views
- : Storage options for the TOAST table
- : Whether the table has any indexes
- : Whether the table has any rules
- : Whether the table has any triggers
- : Whether any columns have non-default ACLs
- : Whether row security is enabled
- : Whether row security is forced
- : Whether the table has OIDs
- : Table's relfrozenxid for VACUUM freeze tracking
- : Table's relminmxid for multixact tracking
- : OID of the associated TOAST table
- : TOAST table's relfrozenxid
- : TOAST table's relminmxid
- : Number of CHECK constraints
- : OID of table's composite type
- : Underlying type for typed tables
- : Foreign server OID for foreign tables
- : OID of table owning this sequence
- : Column number owning this sequence
- : Whether this is an identity sequence
- : Table size in pages
- : TOAST table size in pages

### Processing Control
- : Whether to collect detailed information
- : Whether view definition must be postponed
- : Whether materialized view must be postponed
- : Whether the table is a partition
- : Whether it's an unsafe partitioned table
- : Number of immediate parent tables
- : Array of immediate parent TableInfo structures

### Detailed Attribute Information
- : Number of attributes
- : Attribute names array
- : Attribute type names array
- : Statistics targets for attributes
- : Attribute storage schemes
- : Type storage schemes
- : Whether attributes are dropped
- : Identity column information
- : Generated column information
- : Attribute lengths for binary upgrade
- : Attribute alignment for binary upgrade
- : Whether attributes have local definitions
- : Per-attribute options
- : Per-attribute collation selections
- : Per-attribute compression methods
- : Per-attribute foreign data wrapper options
- : Per-attribute missing values
- : NOT NULL constraints on attributes
- : Whether NOT NULL is inherited
- : DEFAULT expressions for attributes
- : CHECK constraint expressions
- : Whether table has GENERATED ALWAYS AS IDENTITY
- : Access method name

### Dump-Specific Information
- : Number of indexes
- : Array of index information structures
- : Table data information for dumping
- : Number of triggers

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
  - DumpableAcl
  - [_attrDefInfo](../a/_attrDefInfo.md)
  - [_constraintInfo](../c/_constraintInfo.md)
  - [_indxInfo](../i/_indxInfo.md)
  - [_tableDataInfo](_tableDataInfo.md)
  - [_triggerInfo](_triggerInfo.md)
- Called from (representative examples):
  - Self-referential for parent table relationships

## Notes and Other Information
This structure is central to pg_dump's operation and represents the complete metadata model for database tables. The three-tier organization allows for efficient memory usage by only collecting detailed information for tables that are actually needed for the dump operation. The structure supports inheritance relationships, partitioning, foreign tables, and all PostgreSQL table features.