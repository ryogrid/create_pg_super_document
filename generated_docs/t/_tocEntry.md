# _tocEntry

## Location
[src/bin/pg_dump/pg_backup_archiver.h:343-388](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.h#L343-L388)

## Overview
_tocEntry is the core structure that represents individual database objects and their comprehensive metadata within PostgreSQL dump archives, serving as the fundamental building block for the table of contents system.

## Definition


## Detailed Description
The _tocEntry structure is the comprehensive implementation behind the TocEntry typedef, containing all metadata and state information needed to represent and manage individual database objects during dump and restore operations. Each instance represents a single database object (table, index, function, view, etc.) or operation within the archive.

The structure is organized into several logical groups:
1. **List Management**: Circular doubly-linked list pointers for navigation
2. **Object Identity**: Catalog ID, dump ID, and section classification
3. **Object Metadata**: Names, ownership, SQL definitions, and relationships
4. **Dump/Restore State**: Working state variables and processing flags
5. **Parallel Processing**: Dependency tracking for concurrent operations

This design enables efficient dependency resolution, selective restoration, and parallel processing while maintaining referential integrity.

## Parameters / Member Variables
### List Navigation
- : Previous entry in circular linked list
- : Next entry in circular linked list

### Object Identification
- : PostgreSQL catalog identifier for the object
- : Unique identifier within the dump archive
- : Section classification (pre-data, data, post-data)
- : Flag indicating if a dumper routine was provided

### Object Metadata
- : Index tag for object identification
- : Schema name (NULL or empty if not in schema)
- : Tablespace name (NULL if default, empty for database default)
- : Table access method (only for TABLE objects)
- : Relation kind (only for TABLE objects)
- : Object owner name
- : Object description
- : SQL definition statement
- : SQL drop statement
- : SQL copy statement for data

### Dependencies
- : Array of dump IDs this object depends on
- : Number of dependencies

### Dump/Restore Functions
- : Function pointer for dumping object data
- : Argument for the dumper function
- : Format-specific data storage

### Working State
- : Size of object's data (0 if none/unknown)
- : Requirements bit mask (schema/data needed)
- : Flag indicating if object was created (for DATA entries)

### Parallel Processing State
- : Previous link in pending items list
- : Next link in pending items list
- : Number of unresolved dependencies
- : Array of objects depending on this one
- : Number of reverse dependencies
- : Array of objects this one needs locks on
- : Number of lock dependencies

## Dependencies
- Functions called/Symbols referenced:
  - [CatalogId](../C/CatalogId.md) (PostgreSQL catalog identifier type)
  - DumpId (dump-specific identifier type)
  - teSection (table entry section enumeration)
  - pgoff_t (PostgreSQL offset type)
  - DataDumperPtr (function pointer type for data dumping)
  
- Called from (representative examples):
  - [TocEntry](../T/TocEntry.md) (typedef wrapper)
  - _archiveHandle (contains arrays and pointers to entries)
  - All archive format implementations (custom, tar, directory, null)
  - Parallel processing functions throughout pg_dump/pg_restore

## Notes and Other Information
- Forms the backbone of PostgreSQL's dump/restore dependency tracking system
- Circular linked list design enables efficient bidirectional traversal
- Supports complex dependency relationships including reverse dependencies and locking requirements
- Critical for parallel restore operations through dependency counting and state tracking
- The structure is designed to handle all types of database objects uniformly
- Memory management and cleanup are handled by the archive system
- Working state variables are reset/updated during restore operations to track progress