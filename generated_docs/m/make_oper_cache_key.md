# make_oper_cache_key

## Location
[src/backend/parser/parse_oper.c:937-980](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_oper.c#L937-L980)

## Overview
Creates a lookup key structure for operator caching based on operator name and argument types, handling schema resolution and search path management.

## Definition

```c
struct the name list */
	DeconstructQualifiedName(opname, &schemaname, &opername);
```
## Detailed Description
This function prepares a cache key structure (OprCacheKey) for operator lookup operations in PostgreSQL's operator resolution system. It takes an operator name (which may be schema-qualified) and left/right argument type OIDs, then fills in the cache key structure with the necessary information for efficient operator caching.

The function handles two scenarios: when the operator name is schema-qualified (searches only in that specific schema) and when it's unqualified (uses the active search path). If the search path is too long to fit in the cache key structure, caching is disabled for that lookup.

## Parameters / Member Variables
- : Parse state for error reporting context (can be NULL)
- : Output parameter - the OprCacheKey structure to be filled
- : List containing the operator name, possibly schema-qualified
- : OID of the left operand type
- : OID of the right operand type  
- : Location in query for error reporting (-1 if not available)

## Dependencies
- Functions called/Symbols referenced:
  - [DeconstructQualifiedName](../D/DeconstructQualifiedName.md)
  - MemSet
  - [strlcpy](../s/strlcpy.md)
  - [setup_parser_errposition_callback](../s/setup_parser_errposition_callback.md)
  - [LookupExplicitNamespace](../L/LookupExplicitNamespace.md)
  - [cancel_parser_errposition_callback](../c/cancel_parser_errposition_callback.md)
  - [fetch_search_path_array](../f/fetch_search_path_array.md)
- Called from (representative examples):
  - [oper](../o/oper.md) (operator lookup function)
  - [left_oper](../l/left_oper.md) (left unary operator lookup)

## Notes and Other Information
- Returns true on success, false if search path overflow prevents caching
- Ensures zero-fill of the key structure for stable hashing
- Uses NAMEDATALEN for operator name length limits
- MAX_CACHED_PATH_LEN defines the maximum cacheable search path length
- Critical for PostgreSQL's operator resolution performance optimization

## Simplified Source

```c
static bool
make_oper_cache_key(ParseState *pstate, OprCacheKey *key, List *opname,
                    Oid ltypeId, Oid rtypeId, int location)
{
    char *schemaname;
    char *opername;

    // Parse the operator name (may be schema-qualified)
    DeconstructQualifiedName(opname, &schemaname, &opername);

    // Initialize key structure for stable hashing
    MemSet(key, 0, sizeof(OprCacheKey));

    // Store operator name and argument types
    strlcpy(key->oprname, opername, NAMEDATALEN);
    key->left_arg = ltypeId;
    key->right_arg = rtypeId;

    if (schemaname) {
        // Schema-qualified: search only in specified schema
        ParseCallbackState pcbstate;
        setup_parser_errposition_callback(&pcbstate, pstate, location);
        key->search_path[0] = LookupExplicitNamespace(schemaname, false);
        cancel_parser_errposition_callback(&pcbstate);
    }
    else {
        // Unqualified: use current search path
        if (fetch_search_path_array(key->search_path,
                                    MAX_CACHED_PATH_LEN) > MAX_CACHED_PATH_LEN)
            return false;  // Search path too long for caching
    }

    return true;
}
```