# CheckConstraintFetch

## Location
[src/backend/utils/cache/relcache.c:4585-4673](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L4585-L4673)

## Overview
CheckConstraintFetch loads check constraints for a relation from the pg_constraint system catalog and stores them in the relation's cache structure.

## Definition

```c
static void
CheckConstraintFetch(Relation relation)
```
## Detailed Description
CheckConstraintFetch is a static function within the relation cache subsystem that retrieves and processes check constraints for a given relation. The function performs a systematic scan of the pg_constraint catalog to find all check constraints associated with the relation, validates and processes the constraint data, and stores it in the relation's cached tuple descriptor.

The function allocates memory in CacheMemoryContext for storing constraint information, ensuring the data persists for the lifetime of the relation cache entry. It performs validation by checking that the expected number of constraints are found and warns if discrepancies exist. The constraint binary expressions (conbin) are detoasted and converted to C strings for storage.

After loading all constraints, the function sorts them by name to ensure deterministic ordering, which is important for both consistent constraint application and efficient comparison operations in equalTupleDescs().

## Parameters / Member Variables
- : The Relation structure for which check constraints should be loaded

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)  
  - [table_open](../t/table_open.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [table_close](../t/table_close.md)
  - [fastgetattr](../f/fastgetattr.md)
  - TextDatumGetCString
  - [MemoryContextStrdup](../M/MemoryContextStrdup.md)
  - [CheckConstraintCmp](CheckConstraintCmp.md)
  - qsort
- Called from (representative examples):
  - [RelationBuildTupleDesc](../R/RelationBuildTupleDesc.md)

## Notes and Other Information
- Uses CacheMemoryContext for memory allocation to ensure constraint data persists with the relation cache
- Handles missing or extra constraint records gracefully by issuing warnings rather than errors
- Sorts constraints by name for deterministic ordering and performance optimization
- Only processes constraints of type CONSTRAINT_CHECK, ignoring other constraint types
- Validates that conbin (constraint binary expression) is not null before processing