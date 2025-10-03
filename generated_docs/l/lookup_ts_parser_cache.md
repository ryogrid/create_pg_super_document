# lookup_ts_parser_cache

## Location
[src/backend/utils/cache/ts_cache.c:113-207](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/ts_cache.c#L113-L207)

## Overview
Retrieves and caches text search parser configuration information from the PostgreSQL system catalog, providing efficient access to parser function OIDs and metadata.

## Definition

```c
TSParserCacheEntry *
lookup_ts_parser_cache(Oid prsId)
```
## Detailed Description
lookup_ts_parser_cache is the primary function for accessing text search parser information in PostgreSQL. It implements a two-level caching strategy to optimize repeated lookups of parser configurations. The function first checks a single-entry cache (lastUsedParser) for the most recently accessed parser, then falls back to a hash table cache (TSParserCacheHash) for other parsers.

When a parser entry is not found or is invalid, the function performs a system catalog lookup to retrieve the parser definition from pg_ts_parser. It validates that all required parser methods (prsstart, prstoken, prsend) are defined and caches the function manager information for efficient subsequent calls. The function also handles initialization of the hash table cache and registers a syscache callback to maintain cache consistency.

## Parameters / Member Variables
- `prsId`: The Object Identifier (OID) of the text search parser to look up
## Dependencies
- Functions called/Symbols referenced:
  - [hash_create](../h/hash_create.md) (creates the parser cache hash table)
  - [hash_search](../h/hash_search.md) (searches and inserts entries in the hash table)
  - [CacheRegisterSyscacheCallback](../C/CacheRegisterSyscacheCallback.md) (registers cache invalidation callback)
  - [InvalidateTSCacheCallBack](../I/InvalidateTSCacheCallBack.md) (cache invalidation callback function)
  - [CreateCacheMemoryContext](../C/CreateCacheMemoryContext.md) (ensures cache memory context exists)
  - [SearchSysCache1](../S/SearchSysCache1.md) (system catalog lookup)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md) (caches function manager information)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md), PointerGetDatum (datum conversion utilities)
  - HeapTupleIsValid, OidIsValid (validation utilities)
  - MemSet (memory initialization)
- Called from (representative examples):
  - [getTokenTypes](../g/getTokenTypes.md) (in tsearchcmds.c)
  - [parsetext](../p/parsetext.md) (in ts_parse.c)
  - [hlparsetext](../h/hlparsetext.md) (in ts_parse.c)
  - [tt_setup_firstcall](../t/tt_setup_firstcall.md) (in wparser.c)
  - [prs_setup_firstcall](../p/prs_setup_firstcall.md) (in wparser.c)
  - [ts_headline_byid_opt](../t/ts_headline_byid_opt.md) (in wparser.c)

## Notes and Other Information
- The function uses a two-level caching strategy: a single-entry cache for the most recently used parser and a hash table for multiple parsers
- Cache initialization is performed on first access, creating a hash table with 4 initial buckets
- The cache uses CacheMemoryContext for long-lived storage that persists across transactions
- Sanity checks ensure all required parser methods (start, token, end) are defined; headline and lextype methods are optional
- The function registers for pg_ts_parser syscache callbacks to maintain cache consistency across backends
- Function manager information is cached for parser methods to avoid repeated lookups during text search operations
- Cache entries are marked invalid rather than deleted when the underlying catalog changes, allowing for lazy revalidation

## Simplified Source

```c
TSParserCacheEntry *
lookup_ts_parser_cache(Oid prsId)
{
    TSParserCacheEntry *entry;

    // Initialize hash table on first use
    if (TSParserCacheHash == NULL)
    {
        HASHCTL ctl;
        ctl.keysize = sizeof(Oid);
        ctl.entrysize = sizeof(TSParserCacheEntry);
        TSParserCacheHash = hash_create("Tsearch parser cache", 4,
                                        &ctl, HASH_ELEM | HASH_BLOBS);
        // Register for cache invalidation callbacks
        CacheRegisterSyscacheCallback(TSPARSEROID, InvalidateTSCacheCallBack,
                                      PointerGetDatum(TSParserCacheHash));
    }

    // Check single-entry cache first
    if (lastUsedParser && lastUsedParser->prsId == prsId && lastUsedParser->isvalid)
        return lastUsedParser;

    // Try hash table lookup
    entry = (TSParserCacheEntry *) hash_search(TSParserCacheHash, &prsId, HASH_FIND, NULL);

    if (entry == NULL || !entry->isvalid)
    {
        // Load parser from system catalog
        HeapTuple tp = SearchSysCache1(TSPARSEROID, ObjectIdGetDatum(prsId));
        if (!HeapTupleIsValid(tp))
            elog(ERROR, "cache lookup failed for text search parser %u", prsId);

        Form_pg_ts_parser prs = (Form_pg_ts_parser) GETSTRUCT(tp);

        // Validate required methods exist
        if (!OidIsValid(prs->prsstart) || !OidIsValid(prs->prstoken) || !OidIsValid(prs->prsend))
            elog(ERROR, "text search parser %u missing required methods", prsId);

        // Create cache entry if needed
        if (entry == NULL)
            entry = (TSParserCacheEntry *) hash_search(TSParserCacheHash, &prsId, HASH_ENTER, NULL);

        // Populate cache entry
        MemSet(entry, 0, sizeof(TSParserCacheEntry));
        entry->prsId = prsId;
        entry->startOid = prs->prsstart;
        entry->tokenOid = prs->prstoken;
        entry->endOid = prs->prsend;
        entry->headlineOid = prs->prsheadline;
        entry->lextypeOid = prs->prslextype;

        ReleaseSysCache(tp);

        // Cache function manager info for efficient calls
        fmgr_info_cxt(entry->startOid, &entry->prsstart, CacheMemoryContext);
        fmgr_info_cxt(entry->tokenOid, &entry->prstoken, CacheMemoryContext);
        fmgr_info_cxt(entry->endOid, &entry->prsend, CacheMemoryContext);
        if (OidIsValid(entry->headlineOid))
            fmgr_info_cxt(entry->headlineOid, &entry->prsheadline, CacheMemoryContext);

        entry->isvalid = true;
    }

    lastUsedParser = entry;
    return entry;
}
```