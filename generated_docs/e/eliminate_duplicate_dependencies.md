# eliminate_duplicate_dependencies

## Location
[src/backend/catalog/dependency.c:2383-2442](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/dependency.c#L2383-L2442)

## Overview
Optimizes dependency collections by removing duplicate object references and applying intelligent merging rules for whole-object versus partial-object dependencies.

## Definition

```c
static void
eliminate_duplicate_dependencies(ObjectAddresses *addrs)
```
## Detailed Description
This utility function optimizes ObjectAddresses collections by removing redundant dependency references. It performs sophisticated deduplication that goes beyond simple duplicate removal, implementing intelligent merging logic for related object references.

The function first sorts the object addresses to group duplicates together, then processes them with special logic for handling whole-object versus partial-object references. A key optimization is that when both a whole-object reference (objectSubId = 0) and a specific sub-object reference exist for the same object, only the more specific reference is retained.

Key optimizations:
- Exact duplicates are completely removed
- Whole-object references are replaced by specific sub-object references when both exist
- Maintains array compaction for memory efficiency
- Preserves dependency semantics while minimizing storage overhead

This function is critical for maintaining performance in the dependency system, as expression analysis can generate many redundant references that would otherwise create unnecessary catalog entries.

## Parameters / Member Variables
- : ObjectAddresses collection to be deduplicated (modified in-place)

## Dependencies
- Functions called/Symbols referenced:
  - qsort (for sorting object addresses)
  - [object_address_comparator](../o/object_address_comparator.md) (comparison function for sorting)
  - Assert (for validation)
- Called from (representative examples):
  - [recordDependencyOnExpr](../r/recordDependencyOnExpr.md) (after collecting expression dependencies)
  - [recordDependencyOnSingleRelExpr](../r/recordDependencyOnSingleRelExpr.md) (after collecting single-relation dependencies)
  - [record_object_address_dependencies](../r/record_object_address_dependencies.md) (for general dependency optimization)

## Notes and Other Information
- Requires that the ObjectAddresses collection has no 'extras' data to maintain sort sync
- Implements early exit for collections with 0 or 1 references
- Uses in-place modification to minimize memory allocation overhead
- The whole-object vs. partial-object optimization prevents redundant catalog entries
- Critical for performance in systems with complex expressions that reference many objects
- Maintains the sorted order as a side effect, which can be beneficial for downstream processing
- The deduplication logic preserves the most specific dependency relationship available