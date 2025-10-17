# lookup_ts_dictionary_cache

## Location
[src/backend/utils/cache/ts_cache.c:208-361](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/ts_cache.c#L208-L361)

## Overview
Retrieves and caches text search dictionary configuration information, managing both dictionary metadata and private memory contexts for dictionary-specific data.

## Definition

```c
TSDictionaryCacheEntry *
lookup_ts_dictionary_cache(Oid dictId)
```
## Detailed Description
lookup_ts_dictionary_cache manages the caching and initialization of text search dictionaries in PostgreSQL. Unlike parser caching, dictionary caching is more complex because dictionaries have initialization requirements and private data that must persist across calls. The function implements a two-level caching strategy similar to parser caching but adds sophisticated memory management for dictionary-specific contexts.

When a dictionary entry is not cached or invalid, the function performs lookups in both pg_ts_dict and pg_ts_template system catalogs to retrieve the dictionary configuration and its associated template. It validates that required template methods exist and handles dictionary initialization if the template provides an init method. Each dictionary gets its own private memory context for storing initialization data and other dictionary-specific information.

The function registers syscache callbacks for both TSDICTOID and TSTEMPLATEOID to ensure cache consistency when either dictionaries or their templates change.

## Parameters / Member Variables
- `dictId`: The Object Identifier (OID) of the text search dictionary to look up
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

## Simplified Source

```c
TSDictionaryCacheEntry *lookup_ts_dictionary_cache(Oid dictId)
{
    TSDictionaryCacheEntry *entry;

    // Initialize hash table on first use
    if (TSDictionaryCacheHash == NULL) {
        HASHCTL ctl;
        ctl.keysize = sizeof(Oid);
        ctl.entrysize = sizeof(TSDictionaryCacheEntry);
        TSDictionaryCacheHash = hash_create("Tsearch dictionary cache", 8, &ctl, HASH_ELEM | HASH_BLOBS);

        // Register cache invalidation callbacks
        CacheRegisterSyscacheCallback(TSDICTOID, InvalidateTSCacheCallBack, PointerGetDatum(TSDictionaryCacheHash));
        CacheRegisterSyscacheCallback(TSTEMPLATEOID, InvalidateTSCacheCallBack, PointerGetDatum(TSDictionaryCacheHash));

        if (!CacheMemoryContext)
            CreateCacheMemoryContext();
    }

    // Check single-entry cache first
    if (lastUsedDictionary && lastUsedDictionary->dictId == dictId && lastUsedDictionary->isvalid)
        return lastUsedDictionary;

    // Look up existing entry in hash table
    entry = (TSDictionaryCacheEntry *) hash_search(TSDictionaryCacheHash, &dictId, HASH_FIND, NULL);

    if (entry == NULL || !entry->isvalid) {
        // Load dictionary and template from system catalogs
        HeapTuple tpdict = SearchSysCache1(TSDICTOID, ObjectIdGetDatum(dictId));
        if (!HeapTupleIsValid(tpdict))
            elog(ERROR, "cache lookup failed for text search dictionary %u", dictId);

        Form_pg_ts_dict dict = (Form_pg_ts_dict) GETSTRUCT(tpdict);

        HeapTuple tptmpl = SearchSysCache1(TSTEMPLATEOID, ObjectIdGetDatum(dict->dicttemplate));
        if (!HeapTupleIsValid(tptmpl))
            elog(ERROR, "cache lookup failed for text search template %u", dict->dicttemplate);

        Form_pg_ts_template template = (Form_pg_ts_template) GETSTRUCT(tptmpl);

        // Create or reset cache entry
        if (entry == NULL) {
            entry = (TSDictionaryCacheEntry *) hash_search(TSDictionaryCacheHash, &dictId, HASH_ENTER, NULL);
            entry->dictCtx = AllocSetContextCreate(CacheMemoryContext, "TS dictionary", ALLOCSET_SMALL_SIZES);
        } else {
            MemoryContextReset(entry->dictCtx);
        }

        // Initialize entry
        MemSet(entry, 0, sizeof(TSDictionaryCacheEntry));
        entry->dictId = dictId;
        entry->lexizeOid = template->tmpllexize;

        // Call dictionary initialization if available
        if (OidIsValid(template->tmplinit)) {
            MemoryContext oldcontext = MemoryContextSwitchTo(entry->dictCtx);

            // Get dictionary options and call init function
            Datum opt = SysCacheGetAttr(TSDICTOID, tpdict, Anum_pg_ts_dict_dictinitoption, &isnull);
            List *dictoptions = isnull ? NIL : deserialize_deflist(opt);
            entry->dictData = DatumGetPointer(OidFunctionCall1(template->tmplinit, PointerGetDatum(dictoptions)));

            MemoryContextSwitchTo(oldcontext);
        }

        ReleaseSysCache(tptmpl);
        ReleaseSysCache(tpdict);

        // Cache lexize function info
        fmgr_info_cxt(entry->lexizeOid, &entry->lexize, entry->dictCtx);
        entry->isvalid = true;
    }

    lastUsedDictionary = entry;
    return entry;
}
```