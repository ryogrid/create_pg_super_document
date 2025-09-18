# _dumpableObject

## Location
src/bin/pg_dump/pg_dump.h: 141 - 155

## Overview
The  structure serves as the base data structure for all database objects that can be dumped by pg_dump, providing core metadata, dependency tracking, and component control for PostgreSQL schema extraction.

## Definition


## Detailed Description
The  structure is the fundamental base structure used by pg_dump to represent any database object that can be extracted from a PostgreSQL database. It contains essential metadata for object identification, dependency management, and selective dumping capabilities. Every specific object type (tables, functions, types, etc.) in pg_dump extends this base structure, making it the cornerstone of the dump architecture.

The structure manages three key aspects: object identification through catalog IDs and names, dependency relationships between objects to ensure proper dump ordering, and component-based dumping that allows selective extraction of different aspects of objects (definitions, ACLs, comments, etc.).

## Parameters / Member Variables
- : Identifies the specific type of database object (table, function, type, etc.) using the DumpableObjectType enumeration
- : Catalog identifier for objects stored in system catalogs; set to zero for non-cataloged objects
- : Unique identifier assigned by AssignDumpId() function for internal tracking during the dump process
- : Object name string that should never be NULL, used for identification and output generation
- : Pointer to the containing namespace (_namespaceInfo), or NULL for objects not in a specific namespace
- : Bitmask specifying which components of this object should be dumped (definition, ACL, etc.)
- : Bitmask for components of objects contained within this object that should be dumped
- : Bitmask indicating which components are available for dumping from this object
- : Boolean flag indicating whether this object is a member of a PostgreSQL extension
- : Boolean flag indicating whether this object has dependencies on any extension
- : Array of DumpId values representing objects this object depends on for proper ordering
- : Count of valid dependencies currently stored in the dependencies array
- : Allocated size of the dependencies array for memory management

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObjectType
  - CatalogId
  - DumpId
  - _namespaceInfo
  - DumpComponents
- Called from (representative examples):
  - Used as base structure for all specific dumpable object types
  - Referenced by pg_dump's object management functions

## Notes and Other Information
This structure serves as the base for all dumpable objects in pg_dump and is typically extended by specific object type structures. Objects with ACLs must use a DumpableAcl sub-struct that immediately follows this base structure. The dependency management system ensures proper ordering of SQL statements in the dump output, preventing dependency violations during database restoration. The component-based dumping system allows fine-grained control over what aspects of database objects are included in the dump output.