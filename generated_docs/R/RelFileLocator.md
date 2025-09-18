# RelFileLocator

## Location
[src/include/storage/relfilelocator.h:58-63](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/relfilelocator.h#L58-L63)

## Overview
RelFileLocator is a struct that provides all the information needed to physically access a relation's storage files, containing tablespace, database, and relation identifiers.

## Definition


## Detailed Description
The RelFileLocator struct serves as the primary mechanism for identifying the physical storage location of PostgreSQL relations on the filesystem. It uniquely identifies a relation through three key components: tablespace, database, and relation number.

This structure is designed to be used in hashtable keys, requiring that there be no unused padding bytes. Each physical relation may comprise multiple files on the filesystem, as each fork (main, FSM, VM, etc.) is stored as a separate file, and large relations can be divided into multiple segments.

Key constraints and behaviors:
- For shared relations (accessible across all databases), dbOid must be zero and spcOid must be GLOBALTABLESPACE_OID
- The real tablespace ID must always be supplied; shortcuts like reltablespace == 0 are not allowed
- Mapped relations (where relfilenode is zero in pg_class) are not allowed in RelFileLocators
- The relNumber corresponds to pg_class.relfilenode, not pg_class.oid, enabling physical file reassignment

## Parameters / Member Variables
- : Tablespace identifier corresponding to pg_tablespace.oid where the relation is stored
- : Database identifier corresponding to pg_database.oid; zero for shared relations accessible to all databases
- : Relation file number corresponding to pg_class.relfilenode, uniquely identifying the relation within its database and tablespace

## Dependencies
- Functions called/Symbols referenced:
  - [RelFileNumber](RelFileNumber.md) (typedef for Oid)
- Called from (representative examples):
  - Buffer management functions
  - Storage manager operations
  - Cache invalidation routines

## Notes and Other Information
- Used extensively in hashtable keys, requiring careful attention to struct padding
- Cannot represent mapped relations or relations with default tablespace shortcuts
- Works in conjunction with RelFileLocatorBackend when backend process information is needed
- Critical for PostgreSQL's storage abstraction layer and physical file access patterns