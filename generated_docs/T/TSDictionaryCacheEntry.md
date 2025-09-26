# TSDictionaryCacheEntry

## Location
[src/include/tsearch/ts_cache.h:51-63](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tsearch/ts_cache.h#L51-L63)

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
- `dictId`: OID of the text search dictionary (serves as hash lookup key, must be first)
- `isvalid`: Boolean flag indicating cache entry validity (inherited from TSAnyCacheEntry pattern)
- `lexizeOid`: OID of the dictionary's lexize function (most frequently called operation)
- `lexize`: Pre-compiled function manager info for the lexize function
- `dictCtx`: Dedicated memory context for storing dictionary-specific private data
- `*dictData`: Pointer to dictionary's private data structure
## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
  - [FmgrInfo](../F/FmgrInfo.md) (function manager information structure)
  - [MemoryContext](../M/MemoryContext.md) (PostgreSQL memory management context)
- Called from (representative examples):
  - [lookup_ts_dictionary_cache](../l/lookup_ts_dictionary_cache.md)
  - [ts_lexize](../t/ts_lexize.md)
  - [LexizeExec](../L/LexizeExec.md)

## Notes and Other Information
- The dictId field must be first to ensure proper hash table functionality and compatibility with TSAnyCacheEntry casting
- The lexize function is cached as it's the most frequently called dictionary operation during text processing
- The dedicated memory context (dictCtx) allows dictionaries to maintain state across multiple calls, improving performance for stateful dictionaries
- Dictionary private data (dictData) is stored in the dictCtx context to ensure proper memory management
- Cache entries are invalidated when dictionary definitions change in the system catalogs
- Part of PostgreSQL's text search infrastructure that significantly improves dictionary operation performance