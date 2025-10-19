# spgcanreturn

## Location
[src/backend/access/spgist/spgscan.c:1083-1095](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgscan.c#L1083-L1095)

## Overview
Determines whether SPGiST index-only scans can return data for a specific attribute without accessing the heap table.

## Definition

```c
bool
spgcanreturn(Relation index, int attno)
```
## Detailed Description
This function implements the canreturn interface for SPGiST (Space-Partitioned Generalized Search Tree) indexes, determining whether index-only scans are possible for a given attribute. Index-only scans are an optimization where query results can be satisfied entirely from index data without accessing the underlying heap table.

The function checks two conditions: first, it automatically allows index-only scans for INCLUDE attributes (attributes beyond the first indexed column), as these are always stored in the index. For the primary indexed attribute, it consults the operator class configuration to determine if the index implementation supports returning the original data values from the index structure.

## Parameters / Member Variables
- `index`: The SPGiST index relation being queried
- `attno`: Attribute number (1-based) to check for index-only scan capability
## Dependencies
- Functions called/Symbols referenced:
  - [spgGetCache](spgGetCache.md)
- Types used:
  - [Relation](../R/Relation.md)
  - [SpGistCache](../S/SpGistCache.md)
- Called from:
  - [spghandler](spghandler.md) (as part of the access method interface)

## Notes and Other Information
- Returns true if index-only scans are supported for the specified attribute, false otherwise
- INCLUDE attributes (attno > 1) always return true as they are stored in leaf tuples
- For the primary key attribute (attno = 1), the capability depends on the operator class configuration
- Part of PostgreSQL's index-only scan optimization infrastructure
- Essential for query planning decisions regarding whether to use index-only scans
- The canReturnData flag in the operator class configuration determines support for the primary attribute
- Enables significant performance improvements when heap table access can be avoided

## Simplified Source

```c
bool spgcanreturn(Relation index, int attno)
{
    // INCLUDE attributes (beyond primary) can always be fetched
    if (attno > 1)
        return true;

    // Check if opclass config allows returning data for primary attribute
    SpGistCache *cache = spgGetCache(index);
    return cache->config.canReturnData;
}
```