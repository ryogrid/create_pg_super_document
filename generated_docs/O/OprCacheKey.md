# OprCacheKey

## Location
[src/backend/parser/parse_oper.c:51-57](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_oper.c#L51-L57)

## Overview
OprCacheKey is a structure that serves as the hash table lookup key for caching operator resolution results in PostgreSQL's parser.

## Definition

```c
typedef struct OprCacheKey
{
	char		oprname[NAMEDATALEN];
	Oid			left_arg;		/* Left input OID, or 0 if prefix op */
	Oid			right_arg;		/* Right input OID */
	Oid			search_path[MAX_CACHED_PATH_LEN];
} OprCacheKey;
```
## Detailed Description
OprCacheKey is used as the lookup key in PostgreSQL's operator caching mechanism to avoid repeated operator resolution. When the parser needs to resolve an operator (like +, -, etc.), it creates a cache key containing the operator name, argument types, and the current search path. This key is then used to check if the operator has already been resolved and cached, significantly improving performance during query parsing by avoiding redundant catalog lookups.

The structure is designed to be used with PostgreSQL's hash table implementation, where the entire structure serves as a composite key for fast operator lookup.

## Parameters / Member Variables
- : Character array storing the operator name (limited to NAMEDATALEN characters)
- : OID of the left argument type, or 0 for prefix operators (unary operators with right operand only)
- : OID of the right argument type
- : Array of namespace OIDs representing the current search path for operator resolution

## Dependencies
- Functions called/Symbols referenced:
  - NAMEDATALEN
  - MAX_CACHED_PATH_LEN
- Called from (representative examples):
  - [OprCacheEntry](OprCacheEntry.md) (as a member)
  - [make_oper_cache_key](../m/make_oper_cache_key.md)
  - [find_oper_cache_entry](../f/find_oper_cache_entry.md)

## Notes and Other Information
- The structure must be zero-filled before use to ensure stable hashing behavior
- The search path component allows the cache to distinguish between operators with the same name and argument types but resolved in different schema contexts
- Used in conjunction with OprCacheEntry to form a complete cache entry in the operator lookup hash table
- Located in src/backend/parser/parse_oper.c:51-57