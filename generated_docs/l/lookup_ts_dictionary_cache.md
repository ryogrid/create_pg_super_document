# lookup_ts_dictionary_cache

## Location
src/backend/utils/cache/ts_cache.c: 208 - 361

## Overview
Retrieves and caches text search dictionary configuration information, managing both dictionary metadata and private memory contexts for dictionary-specific data.

## Definition


## Detailed Description
lookup_ts_dictionary_cache manages the caching and initialization of text search dictionaries in PostgreSQL. Unlike parser caching, dictionary caching is more complex because dictionaries have initialization requirements and private data that must persist across calls. The function implements a two-level caching strategy similar to parser caching but adds sophisticated memory management for dictionary-specific contexts.

When a dictionary entry is not cached or invalid, the function performs lookups in both pg_ts_dict and pg_ts_template system catalogs to retrieve the dictionary configuration and its associated template. It validates that required template methods exist and handles dictionary initialization if the template provides an init method. Each dictionary gets its own private memory context for storing initialization data and other dictionary-specific information.

The function registers syscache callbacks for both TSDICTOID and TSTEMPLATEOID to ensure cache consistency when either dictionaries or their templates change.

## Parameters / Member Variables
- : The Object Identifier (OID) of the text search dictionary to look up

## Dependencies
- Functions called/Symbols referenced:
  - [hash_create](../h/hash_create.md) (creates the dictionary cache hash table)
  - [hash_search](../h/hash_search.md) (searches and inserts entries in the hash table)
  - [CacheRegisterSyscacheCallback](../C/CacheRegisterSyscacheCallback.md) (registers cache invalidation callbacks for both dict and template catalogs)
  - [InvalidateTSCacheCallBack](../I/InvalidateTSCacheCallBack.md) (cache invalidation callback function)
  - [CreateCacheMemoryContext](../C/CreateCacheMemoryContext.md) (ensures cache memory context exists)
  - [SearchSysCache1](../S/SearchSysCache1.md) (system catalog lookups for both dictionary and template)
  - AllocSetContextCreate (creates private memory context for each dictionary)
  - MemoryContextCopyAndSetIdentifier, MemoryContextSetIdentifier, MemoryContextReset (memory context management)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md) (retrieves dictionary initialization options)
  - deserialize_deflist (parses dictionary options)
  - OidFunctionCall1 (calls dictionary initialization function)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md) (caches function manager information)
- Called from (representative examples):
  - [ts_lexize](../t/ts_lexize.md) (in dict.c)
  - [thesaurus_init](../t/thesaurus_init.md), thesaurus_lexize (in dict_thesaurus.c)
  - [LexizeExec](../L/LexizeExec.md) (in ts_parse.c)

## Notes and Other Information
- Each dictionary maintains its own private memory context (dictCtx) for storing initialization data and dictionary-specific information
- The function handles both dictionary and template catalog changes by registering callbacks for both TSDICTOID and TSTEMPLATEOID
- Dictionary initialization is performed only if the template provides a tmplinit method, with initialization options parsed from the catalog
- Memory context management includes proper cleanup and reinitialization when cache entries are invalidated and rebuilt
- The lexize method from the template is required and cached using function manager information
- Dictionary initialization data is stored in the dictionary's private context and persists across multiple lexize calls
- Cache uses 8 initial buckets compared to 4 for parsers, reflecting the potentially larger number of dictionaries