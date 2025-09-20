# heapam_relation_nontransactional_truncate

## Location
[src/backend/access/heap/heapam_handler.c:627-632](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_handler.c#L627-L632)

## Overview
This function provides a nontransactional truncation mechanism for heap relations, removing all data from the specified relation immediately without transaction logging.

## Definition

```c
enumber value will be caught in
	 * RelationCreateStorage().
	 */
	dstrel = RelationCreateStorage(*newrlocator, rel->rd_rel->relpersistence, true);
```
## Detailed Description
heapam_relation_nontransactional_truncate is a static function that serves as a wrapper around RelationTruncate, specifically designed to truncate a heap relation to block 0 (effectively removing all data). The function operates outside the normal transaction framework, meaning the truncation occurs immediately and is not subject to transaction rollback. This makes it suitable for operations where immediate data removal is required without the overhead of transaction logging.

## Parameters / Member Variables
- `rel`: Relation pointer to the heap relation that should be truncated to empty state

## Dependencies
- Functions called/Symbols referenced:
  - [RelationTruncate](../R/RelationTruncate.md)
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md) (referenced in heapam_handler.c:2629)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the heapam_handler.c file
- The function always truncates to block 0, completely emptying the relation
- Being nontransactional, this operation cannot be rolled back and takes effect immediately
- This function is likely used in scenarios where transactional overhead is undesirable or inappropriate