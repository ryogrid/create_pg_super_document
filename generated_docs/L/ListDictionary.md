# ListDictionary

## Location
[src/include/tsearch/ts_cache.h:69-80](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tsearch/ts_cache.h#L69-L80)

## Overview
A simple data structure that represents a list of dictionary OIDs for text search token processing, used within the PostgreSQL text search configuration cache system.

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
ListDictionary is a lightweight structure that holds an array of dictionary OIDs along with its length. It serves as a building block within the text search configuration caching mechanism, specifically used to store lists of dictionaries associated with different token types in text search configurations. Each text search configuration can have multiple token types, and each token type can be processed by multiple dictionaries in a specific order. This structure encapsulates that ordered list of dictionaries for a particular token type.

The structure is designed for efficient memory management and fast lookups during text search operations, where the system needs to quickly access the appropriate dictionaries for processing different types of tokens.

## Parameters / Member Variables
- `len`: The number of dictionary OIDs stored in the dictIds array
- `dictIds`: Pointer to an array of Oid values representing the dictionary identifiers

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - lookup_ts_config_cache (in TSConfigCacheEntry.map field)
  - LexizeExec (for token processing)

## Notes and Other Information
- Used as an array within TSConfigCacheEntry to represent the complete mapping from token types to dictionary lists
- Memory for the dictIds array is typically allocated in CacheMemoryContext for long-term cache storage
- The structure supports PostgreSQL's text search framework where different token types (like words, emails, URLs) can be processed by different sets of dictionaries in a specific order
- Maximum number of dictionaries per token type is limited by MAXDICTSPERTT constant