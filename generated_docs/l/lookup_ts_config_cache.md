# lookup_ts_config_cache

## Location
[src/backend/utils/cache/ts_cache.c:385-555](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/ts_cache.c#L385-L555)

## Overview
Retrieves and caches complete text search configuration information, including the mapping between token types and their associated dictionaries from the system catalogs.

## Definition

```c
TSConfigCacheEntry *
lookup_ts_config_cache(Oid cfgId)
```
## Detailed Description
lookup_ts_config_cache is the most complex of the text search cache functions, responsible for building and caching complete text search configurations. Unlike parsers and dictionaries which are relatively simple objects, configurations require assembling information from multiple system catalogs (pg_ts_config and pg_ts_config_map) to create a comprehensive mapping structure.

The function follows the familiar two-level caching pattern but adds sophisticated mapping construction. When building a new cache entry, it performs an ordered scan of pg_ts_config_map to collect all dictionary mappings for each token type. The scan leverages the index ordering (mapcfg, maptokentype, mapseqno) to process entries in the correct sequence without explicit sorting.

For each token type, the function maintains an array of dictionary OIDs that will be applied in sequence during text search operations. The final cache entry contains a complete mapping structure that allows fast lookup of dictionaries for any given token type produced by the configuration's parser.

## Parameters / Member Variables
- `cfgId`: The Object Identifier (OID) of the text search configuration to look up
## Dependencies
- Functions called/Symbols referenced:
  - [init_ts_config_cache](../i/init_ts_config_cache.md) (initializes cache infrastructure if needed)
  - [hash_search](../h/hash_search.md) (searches and inserts entries in the hash table)
  - [SearchSysCache1](../S/SearchSysCache1.md) (system catalog lookup for configuration)
  - [table_open](../t/table_open.md), index_open (opens system catalog relations)
  - [systable_beginscan_ordered](../s/systable_beginscan_ordered.md), systable_getnext_ordered, systable_endscan_ordered (ordered system catalog scanning)
  - [index_close](../i/index_close.md), table_close (closes relations)
  - [ScanKeyInit](../S/ScanKeyInit.md) (initializes scan key)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md) (allocates memory for mapping structures)
  - MemSet, memcpy (memory operations)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (datum conversion)
  - MAXTOKENTYPE, MAXDICTSPERTT (token type and dictionary limits)
  - [ListDictionary](../L/ListDictionary.md) (mapping structure)
  - TSConfigMapRelationId, TSConfigMapIndexId (system catalog identifiers)
- Called from (representative examples):
  - [parsetext](../p/parsetext.md), hlparsetext (in ts_parse.c)
  - [ts_headline_byid_opt](../t/ts_headline_byid_opt.md), ts_headline_jsonb_byid_opt, ts_headline_json_byid_opt (in wparser.c)

## Notes and Other Information
- The function builds complex mapping structures that associate token types with ordered lists of dictionaries
- Uses ordered system catalog scans to ensure proper sequence of dictionary application for each token type
- Memory allocation for mapping structures occurs in CacheMemoryContext for persistence across transactions
- The cache entry cleanup code properly deallocates nested dictionary arrays when rebuilding invalid entries
- Token type validation ensures values are within the valid range (1 to MAXTOKENTYPE)
- Enforces limits on the number of dictionaries per token type (MAXDICTSPERTT) to prevent excessive resource usage
- The mapping structure enables efficient lookup during text search operations by pre-organizing dictionary sequences
- Cache initialization is handled through the separate init_ts_config_cache function to support early callback registration
- The function validates that the configuration has a valid parser before proceeding with mapping construction