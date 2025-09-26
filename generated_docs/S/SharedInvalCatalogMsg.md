# SharedInvalCatalogMsg

## Location
[src/include/storage/sinval.h:74-75](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/sinval.h#L74-L75)

## Overview
SharedInvalCatalogMsg is a structure that represents a shared invalidation message for invalidating all catcache entries from a given system catalog across PostgreSQL processes.

## Definition

```c
typedef struct
{
	int8		id;				/* type field --- must be first */
	Oid			dbId;			/* database ID, or 0 if a shared relation */
	Oid			relId;			/* relation ID, or 0 if whole relcache */
} SharedInvalRelcacheMsg;
```
## Detailed Description
SharedInvalCatalogMsg is part of PostgreSQL's shared invalidation system, specifically designed to handle bulk invalidation of catalog cache entries. Unlike SharedInvalCatcacheMsg which targets specific cached tuples, this structure invalidates all cached entries belonging to an entire system catalog. This is used when changes affect multiple or all entries in a catalog, making it more efficient to invalidate the entire catalog cache rather than individual entries.

The structure supports both database-specific catalogs and shared system catalogs. When a catalog undergoes significant changes (such as during DDL operations that affect metadata structure), this message type ensures all backend processes clear their cached catalog information for consistency.

## Parameters / Member Variables
- uid=1000(ryo) gid=1000(ryo) groups=1000(ryo),4(adm),20(dialout),24(cdrom),25(floppy),27(sudo),29(audio),30(dip),44(video),46(plugdev),117(netdev),998(ollama),999(docker): Type field that must be the first member to identify this as a catalog invalidation message (set to SHAREDINVALCATALOG_ID which is -1)
- : Database ID for database-specific catalogs, or 0 for shared system catalogs that apply across all databases
- : Object identifier of the specific catalog whose cached contents should be invalidated

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
  - int8 (PostgreSQL integer type)
  - SHAREDINVALCATALOG_ID (constant defined as -1)
- Called from (representative examples):
  - SharedInvalidationMessage (union containing this structure)
  - Catalog invalidation functions in the sinval subsystem

## Notes and Other Information
- The uid=1000(ryo) gid=1000(ryo) groups=1000(ryo),4(adm),20(dialout),24(cdrom),25(floppy),27(sudo),29(audio),30(dip),44(video),46(plugdev),117(netdev),998(ollama),999(docker) field is set to SHAREDINVALCATALOG_ID (-1) to distinguish catalog invalidation messages from other message types
- This structure is part of the SharedInvalidationMessage union that encompasses all invalidation message types
- More efficient than individual tuple invalidation when multiple entries in a catalog need to be invalidated
- Used during DDL operations, system catalog modifications, and other operations that affect catalog metadata
- The distinction between shared (dbId = 0) and database-specific catalogs allows proper scoping of invalidation messages
- Part of PostgreSQL's multi-process cache coherency mechanism