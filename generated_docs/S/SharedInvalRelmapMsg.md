# SharedInvalRelmapMsg

## Location
[src/include/storage/sinval.h:102-103](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/sinval.h#L102-L103)

## Overview
SharedInvalRelmapMsg is a structure that represents a shared invalidation message for invalidating the mapped-relation mapping for a given database across PostgreSQL processes.

## Definition

```c
typedef struct
{
	int8		id;				/* type field --- must be first */
	Oid			dbId;			/* database ID, or 0 if a shared relation */
	Oid			relId;			/* relation ID */
} SharedInvalSnapshotMsg;
```
## Detailed Description
SharedInvalRelmapMsg is part of PostgreSQL's shared invalidation system, specifically designed to handle invalidation of relation mapping information. The relation mapping system maintains the correspondence between logical relation OIDs and their physical file numbers, which is especially important for critical system catalogs that have fixed OIDs but variable file numbers.

This structure is used when the mapping between relation OIDs and physical files changes, which can occur during operations like CLUSTER, REINDEX of system catalogs, or other operations that result in relation file relocations. The structure supports both database-specific mappings and shared catalog mappings.

## Parameters / Member Variables
- uid=1000(ryo) gid=1000(ryo) groups=1000(ryo),4(adm),20(dialout),24(cdrom),25(floppy),27(sudo),29(audio),30(dip),44(video),46(plugdev),117(netdev),998(ollama),999(docker): Type field that must be the first member to identify this as a relation mapping invalidation message (set to SHAREDINVALRELMAP_ID which is -4)
- : Database ID for database-specific relation mappings, or 0 for shared system catalogs that apply across all databases

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
  - int8 (PostgreSQL integer type)
  - SHAREDINVALRELMAP_ID (constant defined as -4)
- Called from (representative examples):
  - [SharedInvalidationMessage](SharedInvalidationMessage.md) (union containing this structure)
  - [Relation](../R/Relation.md) mapping invalidation functions in the sinval subsystem

## Notes and Other Information
- The uid=1000(ryo) gid=1000(ryo) groups=1000(ryo),4(adm),20(dialout),24(cdrom),25(floppy),27(sudo),29(audio),30(dip),44(video),46(plugdev),117(netdev),998(ollama),999(docker) field is set to SHAREDINVALRELMAP_ID (-4) to distinguish relation mapping invalidation messages from other message types
- This is the simplest of the shared invalidation message structures, containing only identification fields
- Used when the physical-to-logical relation mapping changes, ensuring all processes update their cached mapping information
- Critical for maintaining consistency of relation file access across multiple PostgreSQL backend processes
- Part of the SharedInvalidationMessage union that encompasses all invalidation message types
- Particularly important for system catalogs that have special handling in the relation mapping system
- The distinction between shared (dbId = 0) and database-specific mappings ensures proper scoping of invalidation
- Used during operations that can change the physical file location of relations while maintaining their logical identity