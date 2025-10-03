# RelationIdIsInInitFile

## Location
[src/backend/utils/cache/relcache.c:6726-6765](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L6726-L6765)

## Overview
Determines whether a given relation (identified by OID) should be stored in the local relcache initialization file for fast backend startup.

## Definition

```c
bool
RelationIdIsInInitFile(Oid relationId)
```
## Detailed Description
This function implements the filtering logic that determines which relations should be included in the local database initialization file. The goal is to include relations that are frequently accessed during backend startup to optimize performance.

The function implements a two-tier approach:
1. All relations that support system caches (syscaches) are included, as these are frequently accessed
2. Special case relations that are nailed for efficiency reasons but don't support syscaches are explicitly included

The special cases handle specific system relations that are critical for database operations but don't fit the standard syscache pattern. These include relations for shared security labels, triggers, databases, and their associated indexes.

The function serves as a gate-keeper to ensure that only performance-critical relations are cached in the initialization file, balancing startup speed against file size and maintenance overhead.

## Parameters / Member Variables
- `relationId`: The OID of the relation to check
## Dependencies
- Functions called/Symbols referenced:
  - [RelationSupportsSysCache](RelationSupportsSysCache.md)
  - SharedSecLabelRelationId (constant)
  - TriggerRelidNameIndexId (constant)
  - DatabaseNameIndexId (constant)
  - SharedSecLabelObjectIndexId (constant)
- Called from (representative examples):
  - [write_relcache_init_file](../w/write_relcache_init_file.md)
  - [RegisterRelcacheInvalidation](RegisterRelcacheInvalidation.md)

## Notes and Other Information
- Returns true if the relation should be included in the init file, false otherwise
- Special case relations are explicitly listed and verified to not support syscaches via Assert
- The comment indicates this set of special cases may change over time as the codebase evolves
- Primary criteria: nailed relations + syscache-supporting relations + specific exceptions
- Used by the invalidation system to determine which init files need to be invalidated
- File location: src/backend/utils/cache/relcache.c:6726-6765

## Simplified Source

```c
bool
RelationIdIsInInitFile(Oid relationId)
{
    // Special case relations that are nailed but don't support syscache
    if (relationId == SharedSecLabelRelationId ||
        relationId == TriggerRelidNameIndexId ||
        relationId == DatabaseNameIndexId ||
        relationId == SharedSecLabelObjectIndexId)
    {
        // Verify these special cases don't support syscache
        Assert(!RelationSupportsSysCache(relationId));
        return true;
    }

    // Include all relations that support system caches
    return RelationSupportsSysCache(relationId);
}
```