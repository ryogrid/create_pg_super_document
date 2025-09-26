# TSDictionaryCacheEntry

## Location
src/include/tsearch/ts_cache.h: 51 - 63

## Overview
TSDictionaryCacheEntry is a cache structure that stores precompiled function information and private data for text search dictionaries, optimizing dictionary operations by avoiding repeated lookups and maintaining persistent dictionary state.

## Definition

```c
typedef struct TSDictionaryCacheEntry
{
	/* dictId is the hash lookup key and MUST BE FIRST */
	Oid			dictId;
	bool		isvalid;

	/* most frequent fmgr call */
	Oid			lexizeOid;
	FmgrInfo	lexize;

	MemoryContext dictCtx;		/* memory context to store private data */
	void	   *dictData;
} TSDictionaryCacheEntry;
```
## Detailed Description
TSDictionaryCacheEntry extends the TSAnyCacheEntry pattern to cache essential information about text search dictionaries. It stores the precompiled lexize function (the most frequently called dictionary operation) and maintains a dedicated memory context for dictionary-specific private data.

The structure follows the common header pattern with dictId and isvalid at the beginning, ensuring compatibility with generic cache operations. The dictCtx memory context allows dictionaries to maintain persistent state across calls, while the cached FmgrInfo structure eliminates function lookup overhead for the lexize operation.

## Parameters / Member Variables
- : OID of the text search dictionary (serves as hash lookup key, must be first)
- : Boolean flag indicating cache entry validity (inherited from TSAnyCacheEntry pattern)
- : OID of the dictionary's lexize function (most frequently called operation)
- : Pre-compiled function manager info for the lexize function
- : Dedicated memory context for storing dictionary-specific private data
- : Pointer to dictionary's private data structure

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
  - FmgrInfo (function manager information structure)
  - MemoryContext (PostgreSQL memory management context)
- Called from (representative examples):
  - lookup_ts_dictionary_cache
  - ts_lexize
  - LexizeExec

## Notes and Other Information
- The dictId field must be first to ensure proper hash table functionality and compatibility with TSAnyCacheEntry casting
- The lexize function is cached as it's the most frequently called dictionary operation during text processing
- The dedicated memory context (dictCtx) allows dictionaries to maintain state across multiple calls, improving performance for stateful dictionaries
- Dictionary private data (dictData) is stored in the dictCtx context to ensure proper memory management
- Cache entries are invalidated when dictionary definitions change in the system catalogs
- Part of PostgreSQL's text search infrastructure that significantly improves dictionary operation performance