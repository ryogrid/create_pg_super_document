# RelationGetFKeyList

## Location
src/backend/utils/cache/relcache.c: 4697 - 4805

## Overview
RelationGetFKeyList returns a list of foreign key constraints for a given relation by scanning pg_constraint and building ForeignKeyCacheInfo structures.

## Definition
```c
List *RelationGetFKeyList(Relation relation)
```

## Detailed Description
RelationGetFKeyList retrieves all foreign key constraints that reference the given relation as the referencing table. The function implements a caching mechanism - if the foreign key list has already been computed and cached (rd_fkeyvalid is true), it returns the cached list immediately.

For performance optimization, the function includes a fast path that returns NIL immediately for tables that cannot have foreign keys (non-partitioned tables without triggers).

When building the foreign key list, the function scans pg_constraint for CONSTRAINT_FOREIGN entries where conrelid matches the target relation. For each foreign key found, it creates a ForeignKeyCacheInfo structure containing the constraint OID, referencing relation OID, referenced relation OID, and detailed constraint information extracted via DeconstructFkConstraintRow.

The function carefully manages memory contexts to avoid leaks: it builds the result list in the caller's context, then copies it to CacheMemoryContext for caching. The cached list persists until the relcache entry is reset.

## Parameters / Member Variables
- `relation`: The Relation structure for which foreign key constraints should be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - ScanKeyInit
  - table_open
  - systable_beginscan
  - systable_getnext
  - systable_endscan
  - table_close
  - makeNode
  - DeconstructFkConstraintRow
  - lappend
  - MemoryContextSwitchTo
  - copyObject
  - list_free_deep
  - ForeignKeyCacheInfo (struct type)
- Called from (representative examples):
  - addFkRecurseReferencing
  - CloneFkReferencing
  - DetachPartitionFinalize
  - get_relation_foreign_keys

## Notes and Other Information
- Implements caching via rd_fkeylist and rd_fkeyvalid fields in the relation structure
- Returns data directly from relcache - callers must be careful about cache invalidation
- Fast path optimization for tables that cannot have foreign keys
- Uses DeconstructFkConstraintRow to extract detailed constraint information from pg_constraint tuples
- Memory management prevents leaks by building lists in appropriate contexts
- List items are returned in no particular order
- Callers should use copyObject() if they need to retain the list across potential cache flushes