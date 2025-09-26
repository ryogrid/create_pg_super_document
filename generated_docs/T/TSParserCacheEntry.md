# TSParserCacheEntry

## Location
[src/include/tsearch/ts_cache.h:30-49](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tsearch/ts_cache.h#L30-L49)

## Overview
TSParserCacheEntry is a cache structure that stores precompiled function call information for text search parsers, optimizing performance by avoiding repeated function lookups during parsing operations.

## Definition

```c
typedef struct TSParserCacheEntry
{
	/* prsId is the hash lookup key and MUST BE FIRST */
	Oid			prsId;			/* OID of the parser */
	bool		isvalid;

	Oid			startOid;
	Oid			tokenOid;
	Oid			endOid;
	Oid			headlineOid;
	Oid			lextypeOid;

	/*
	 * Pre-set-up fmgr call of most needed parser's methods
	 */
	FmgrInfo	prsstart;
	FmgrInfo	prstoken;
	FmgrInfo	prsend;
	FmgrInfo	prsheadline;
} TSParserCacheEntry;
```
## Detailed Description
TSParserCacheEntry extends TSAnyCacheEntry to cache essential information about text search parsers. It stores both the OIDs of parser functions and their pre-compiled FmgrInfo structures, eliminating the need for repeated function lookups during text parsing operations.

The structure follows the common header pattern by placing prsId and isvalid at the beginning, making it compatible with the generic cache invalidation mechanism. The cached FmgrInfo structures significantly improve performance by avoiding the overhead of function manager lookups for each parsing operation.

## Parameters / Member Variables
- `prsId`: OID of the text search parser (serves as hash lookup key, must be first)
- `isvalid`: Boolean flag indicating cache entry validity (inherited from TSAnyCacheEntry pattern)
- `startOid`: OID of the parser's start function
- `tokenOid`: OID of the parser's token extraction function
- `endOid`: OID of the parser's end function
- `headlineOid`: OID of the parser's headline generation function
- `lextypeOid`: OID of the parser's lexical type function
- `prsstart`: Pre-compiled function manager info for the start function
- `prstoken`: Pre-compiled function manager info for the token function
- `prsend`: Pre-compiled function manager info for the end function
- `prsheadline`: Pre-compiled function manager info for the headline function
## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
  - [FmgrInfo](../F/FmgrInfo.md) (function manager information structure)
- Called from (representative examples):
  - [lookup_ts_parser_cache](../l/lookup_ts_parser_cache.md)
  - [parsetext](../p/parsetext.md)
  - [hlparsetext](../h/hlparsetext.md)
  - [ts_headline_byid_opt](../t/ts_headline_byid_opt.md)

## Notes and Other Information
- The prsId field must be first to ensure proper hash table functionality and compatibility with TSAnyCacheEntry casting
- [FmgrInfo](../F/FmgrInfo.md) structures are expensive to initialize, so caching them provides significant performance benefits
- Used extensively in text search parsing operations where parser functions are called repeatedly
- Part of PostgreSQL's text search caching infrastructure that reduces function lookup overhead
- Cache entries are invalidated when parser definitions change in the system catalogs