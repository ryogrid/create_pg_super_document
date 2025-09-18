# RelationReloadIndexInfo

## Location
src/backend/utils/cache/relcache.c: 2257 - 2370

## Overview
Reloads minimal information for an invalidated index relation without performing a complete cache rebuild, supporting only specific schema changes allowed for existing indexes.

## Definition
```c
static void RelationReloadIndexInfo(Relation relation)
```

## Detailed Description
RelationReloadIndexInfo is a specialized function designed to handle relcache invalidation events for index relations. When an index receives an invalidation signal (typically due to changes in pg_class or pg_index), this function selectively updates the cached information without performing a costly complete rebuild.

The function supports two main categories of updates:
1. **pg_class updates**: Complete replacement of the pg_class row data, including reloption parsing and physical address recalculation
2. **pg_index boolean field updates**: Selective copying of boolean fields that are allowed to change (like indisvalid, indisready, etc.)

Key design considerations:
- Cannot perform complete rebuilds for "nailed" indexes or those in active use
- Handles failed transaction scenarios where catalog reads might not be immediately possible
- Includes special handling for shared indexes during backend startup
- Avoids deadlock risks when updating system catalog indexes

## Parameters / Member Variables
- `relation`: Pointer to the invalidated index Relation structure that needs reloading

## Dependencies
- Functions called/Symbols referenced:
  - RelationCloseSmgr
  - [ScanPgRelation](../S/ScanPgRelation.md)
  - [RelationParseRelOptions](RelationParseRelOptions.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [RelationInitPhysicalAddr](RelationInitPhysicalAddr.md)
  - [IsSystemRelation](../I/IsSystemRelation.md)
  - HeapTupleHeaderSetXmin
- Called from (representative examples):
  - [RelationIdGetRelation](RelationIdGetRelation.md)
  - [RelationReloadNailed](RelationReloadNailed.md)
  - [RelationClearRelation](RelationClearRelation.md)

## Notes and Other Information
- This is a static function, only callable from within relcache.c
- Requires AccessShareLock on the target index at call time
- For system catalog indexes, special deadlock prevention measures are needed
- Only updates boolean fields from pg_index; array fields are immutable for existing indexes
- Shared indexes receive minimal handling during backend startup when critical relcaches aren't built yet
- The function preserves expensive-to-rebuild information like support function lookup data
- AM (Access Method) cached data is cleared to ensure consistency after reload
- Includes assertion checks to ensure it's only called on appropriate index types in invalid state