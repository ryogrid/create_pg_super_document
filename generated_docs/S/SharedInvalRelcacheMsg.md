# SharedInvalRelcacheMsg

## Location
[src/include/storage/sinval.h:83-84](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/sinval.h#L83-L84)

## Overview
SharedInvalRelcacheMsg is a structure that represents a shared invalidation message for invalidating relcache (relation cache) entries for specific logical relations or the entire relcache across PostgreSQL processes.

## Definition

```c
typedef struct
{
	/* note: field layout chosen to pack into 16 bytes */
	int8		id;				/* type field --- must be first */
	int8		backend_hi;		/* high bits of backend procno, if temprel */
	uint16		backend_lo;		/* low bits of backend procno, if temprel */
	RelFileLocator rlocator;	/* spcOid, dbOid, relNumber */
} SharedInvalSmgrMsg;
```
## Detailed Description
SharedInvalRelcacheMsg is part of PostgreSQL's shared invalidation system, designed to handle invalidation of relation cache entries. The relation cache (relcache) stores metadata about tables, indexes, and other database relations to avoid repeated lookups of system catalogs. This structure supports both targeted invalidation of specific relations and bulk invalidation of the entire relcache.

When relation metadata changes (such as during ALTER TABLE operations, index creation/deletion, or permission changes), this message type ensures all backend processes update their cached relation information. The structure can invalidate either a single relation's cache entry or the entire relcache depending on the scope of changes.

## Parameters / Member Variables
- uid=1000(ryo) gid=1000(ryo) groups=1000(ryo),4(adm),20(dialout),24(cdrom),25(floppy),27(sudo),29(audio),30(dip),44(video),46(plugdev),117(netdev),998(ollama),999(docker): Type field that must be the first member to identify this as a relcache invalidation message (set to SHAREDINVALRELCACHE_ID which is -2)
- : Database ID for database-specific relations, or 0 for shared relations that exist across all databases
- : Relation ID of the specific relation whose cache entry should be invalidated, or 0 to invalidate the entire relcache

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
  - int8 (PostgreSQL integer type)
  - SHAREDINVALRELCACHE_ID (constant defined as -2)
- Called from (representative examples):
  - [SharedInvalidationMessage](SharedInvalidationMessage.md) (union containing this structure)
  - Relcache invalidation functions in the sinval subsystem

## Notes and Other Information
- The uid=1000(ryo) gid=1000(ryo) groups=1000(ryo),4(adm),20(dialout),24(cdrom),25(floppy),27(sudo),29(audio),30(dip),44(video),46(plugdev),117(netdev),998(ollama),999(docker) field is set to SHAREDINVALRELCACHE_ID (-2) to distinguish relcache invalidation messages from other message types
- This structure is part of the SharedInvalidationMessage union that encompasses all invalidation message types
- Setting  to 0 invalidates the entire relcache, which is more efficient when many relations are affected
- Used during DDL operations, permission changes, and other operations that modify relation metadata
- Supports both shared relations (system catalogs, shared tables) and database-specific relations
- Critical for maintaining consistency of relation metadata across multiple PostgreSQL backend processes
- Part of PostgreSQL's cache coherency mechanism that prevents stale metadata from being used