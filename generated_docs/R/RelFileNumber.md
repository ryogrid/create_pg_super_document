# RelFileNumber

## Location
src/include/common/relpath.h: 25 - 25

## Overview
RelFileNumber is a data type that identifies specific relation file names in PostgreSQL's storage system, essentially serving as a unique identifier for physical files that store table and index data.

## Definition

Located at src/include/common/relpath.h:25

## Detailed Description
RelFileNumber is a typedef that wraps the fundamental Oid type (unsigned int) to provide semantic meaning for relation file identification. This type is used throughout PostgreSQL's storage layer to uniquely identify the physical files that contain relation data. Each table, index, or other relation in PostgreSQL has an associated RelFileNumber that corresponds to the actual filename in the database directory structure.

The RelFileNumber serves as a bridge between PostgreSQL's logical relation identifiers and the physical file system, enabling the database to locate and manage the correct files for each relation. This abstraction allows for operations like relation file renaming, tablespace moves, and file system maintenance while maintaining referential integrity.

## Parameters / Member Variables
N/A - This is a simple typedef, not a function or structure.

## Dependencies
- Functions called/Symbols referenced:
  - Oid (underlying type)
  - InvalidOid (used in InvalidRelFileNumber constant)

- Called from (representative examples):
  - [heap_create](../h/heap_create.md) (creates new relation files)
  - index_create (creates new index files)  
  - [RelationInitPhysicalAddr](RelationInitPhysicalAddr.md) (initializes physical addresses)
  - [GetRelationPath](../G/GetRelationPath.md) (constructs file paths)
  - [swap_relation_files](../s/swap_relation_files.md) (exchanges relation files during operations like CLUSTER)
  - [RelationSetNewRelfilenumber](RelationSetNewRelfilenumber.md) (assigns new file numbers to relations)
  - [RelationMapFilenumberToOid](RelationMapFilenumberToOid.md) (maps file numbers to object IDs)

## Notes and Other Information
- The InvalidRelFileNumber constant is defined as ((RelFileNumber) InvalidOid) and represents an invalid or uninitialized file number
- [RelFileNumber](RelFileNumber.md) is extensively used in buffer management (BufferTag), backup operations, and storage management
- This type is fundamental to PostgreSQL's MVCC implementation and physical storage architecture
- Used in conjunction with ForkNumber to fully specify relation file variants (main, FSM, visibility map, etc.)
- Critical for binary upgrade processes where file number consistency must be maintained across PostgreSQL versions