# TSParserCacheEntry

## Location
src/include/tsearch/ts_cache.h: 30 - 49

## Overview
TSParserCacheEntry is a cache structure that stores precompiled function call information for text search parsers, optimizing performance by avoiding repeated function lookups during parsing operations.

## Definition


## Detailed Description
TSParserCacheEntry extends TSAnyCacheEntry to cache essential information about text search parsers. It stores both the OIDs of parser functions and their pre-compiled FmgrInfo structures, eliminating the need for repeated function lookups during text parsing operations.

The structure follows the common header pattern by placing prsId and isvalid at the beginning, making it compatible with the generic cache invalidation mechanism. The cached FmgrInfo structures significantly improve performance by avoiding the overhead of function manager lookups for each parsing operation.

## Parameters / Member Variables
- : OID of the text search parser (serves as hash lookup key, must be first)
- : Boolean flag indicating cache entry validity (inherited from TSAnyCacheEntry pattern)
- : OID of the parser's start function
- : OID of the parser's token extraction function  
- : OID of the parser's end function
- : OID of the parser's headline generation function
- : OID of the parser's lexical type function
- : Pre-compiled function manager info for the start function
- : Pre-compiled function manager info for the token function
- : Pre-compiled function manager info for the end function
- : Pre-compiled function manager info for the headline function

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
  - FmgrInfo (function manager information structure)
- Called from (representative examples):
  - lookup_ts_parser_cache
  - parsetext
  - hlparsetext
  - ts_headline_byid_opt

## Notes and Other Information
- The prsId field must be first to ensure proper hash table functionality and compatibility with TSAnyCacheEntry casting
- FmgrInfo structures are expensive to initialize, so caching them provides significant performance benefits
- Used extensively in text search parsing operations where parser functions are called repeatedly
- Part of PostgreSQL's text search caching infrastructure that reduces function lookup overhead
- Cache entries are invalidated when parser definitions change in the system catalogs