# CacheInvalidateRelcache

## Location
[src/backend/utils/cache/inval.c:1363-1386](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/inval.c#L1363-L1386)

## Overview
Registers invalidation of a specified relation's relcache entry at the end of the current command, used to force relcache rebuilds when changes don't affect recognized cache contributors.

## Definition

```c
void
CacheInvalidateRelcache(Relation relation)
```
## Detailed Description
CacheInvalidateRelcache is used to invalidate the relcache (relation cache) entry for a specific relation. This function is called in situations where operations need to force a relcache rebuild but aren't changing any of the tuples that are normally recognized as contributors to the relcache entry by CacheInvalidateHeapTuple. A prime example is dropping an index, which affects the relation's structure but doesn't modify the main relation tuple itself.

The function extracts the relation OID from the relation structure and determines the appropriate database context (shared vs. database-specific), then registers the invalidation message to be processed at command end.

## Parameters / Member Variables
- : Pointer to the Relation structure whose relcache entry should be invalidated

## Dependencies
- Functions called/Symbols referenced:
  - [PrepareInvalidationState](../P/PrepareInvalidationState.md)
  - RelationGetRelid (macro)
  - [RegisterRelcacheInvalidation](../R/RegisterRelcacheInvalidation.md)
- Called from (representative examples):
  - [index_create](../i/index_create.md)
  - [index_drop](../i/index_drop.md)
  - [SetRelationNumChecks](../S/SetRelationNumChecks.md)
  - [RemoveTriggerById](../R/RemoveTriggerById.md)
  - [CreatePolicy](CreatePolicy.md)
  - [AlterPolicy](../A/AlterPolicy.md)

## Notes and Other Information
- This function handles both shared and database-specific relations by checking the relisshared flag
- It's used extensively throughout the catalog management code for operations that modify relation metadata indirectly
- The invalidation is deferred until command end to avoid performance issues with multiple invalidations during complex operations
- This is more targeted than CacheInvalidateCatalog as it affects only one specific relation rather than an entire catalog