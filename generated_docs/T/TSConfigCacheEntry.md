# TSConfigCacheEntry

## Location
[src/include/tsearch/ts_cache.h:81-96](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tsearch/ts_cache.h#L81-L96)

## Overview
A cache entry structure that stores text search configuration metadata and token-to-dictionary mappings for efficient text search processing in PostgreSQL.

## Definition

```c
typedef struct
{
	/* cfgId is the hash lookup key and MUST BE FIRST */
	Oid			cfgId;
	bool		isvalid;

	Oid			prsId;

	int			lenmap;
	ListDictionary *map;
} TSConfigCacheEntry;
```
## Detailed Description
TSConfigCacheEntry is a core data structure in PostgreSQL's text search caching system that represents a cached text search configuration. It stores essential information about how text should be parsed and processed, including the parser to use and the mapping from token types to dictionaries.

This structure is designed for high-performance text search operations by caching frequently accessed configuration data in memory. The cache eliminates the need to repeatedly query system catalogs (pg_ts_config and pg_ts_config_map) during text search operations. The structure follows PostgreSQL's hash table conventions with the lookup key (cfgId) positioned as the first field.

The map field contains an array of ListDictionary structures, where each array index corresponds to a token type, and each ListDictionary contains the ordered list of dictionaries that should process that token type.

## Parameters / Member Variables
- `cfgId`: The OID of the text search configuration (hash lookup key, must be first field)
- `isvalid`: Flag indicating whether this cache entry contains valid data
- `prsId`: The OID of the text search parser associated with this configuration
- `lenmap`: The length of the map array (typically MAXTOKENTYPE + 1)
- `map`: Pointer to an array of ListDictionary structures mapping token types to dictionary lists

## Dependencies
- Functions called/Symbols referenced:
  - [ListDictionary](../L/ListDictionary.md) (for token type to dictionary mapping)
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - [lookup_ts_config_cache](../l/lookup_ts_config_cache.md) (main cache lookup function)
  - [LexizeInit](../L/LexizeInit.md) (text search parsing initialization)
  - [parsetext](../p/parsetext.md) (text parsing operations)
  - [ts_headline_byid_opt](../t/ts_headline_byid_opt.md) (headline generation)

## Notes and Other Information
- Used as entries in TSConfigCacheHash, PostgreSQL's global text search configuration cache
- Memory for the map array is allocated in CacheMemoryContext for long-term persistence
- Cache entries are invalidated automatically when pg_ts_config or pg_ts_config_map system catalogs change
- The structure supports a single-entry cache optimization (lastUsedConfig) for frequently accessed configurations
- Maximum token type supported is limited by MAXTOKENTYPE constant
- The cfgId field's position as the first member is critical for hash table operations - this requirement is explicitly documented in the code