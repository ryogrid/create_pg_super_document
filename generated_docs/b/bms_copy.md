# bms_copy

## Location
[src/backend/nodes/bitmapset.c:122-141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/bitmapset.c#L122-L141)

## Overview
Creates a palloc allocated copy of a Bitmapset, providing deep copy functionality for bitmap set data structures.

## Definition
Bitmapset *bms_copy(const Bitmapset *a)

## Detailed Description
bms_copy is a fundamental function that creates a complete deep copy of a Bitmapset. It allocates new memory using palloc and performs a bitwise copy of the entire structure, including all words in the bitmap. The function properly handles the NULL case, where NULL represents an empty set, and maintains the invariant that copied sets are completely independent of their originals.

The function uses memcpy for efficient copying of the entire bitmap structure, ensuring that all bits and metadata are preserved in the new copy. This is essential for scenarios where multiple independent copies of a set are needed for different operations or when the original set needs to be preserved while creating modified versions.

## Parameters / Member Variables
- `a`: A constant pointer to the Bitmapset to copy. Can be NULL (representing an empty set).

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_valid_set](bms_is_valid_set.md) (validation in Assert)
  - BITMAPSET_SIZE (macro for calculating bitmap size)
  - [palloc](../p/palloc.md) (memory allocation)
  - memcpy (memory copying)
- Called from (representative examples):
  - [afterTriggerCopyBitmap](../a/afterTriggerCopyBitmap.md)
  - [CreatePartitionPruneState](../C/CreatePartitionPruneState.md)
  - [ExecFindMatchingSubPlans](../E/ExecFindMatchingSubPlans.md)
  - [bms_copy_and_free](bms_copy_and_free.md)
  - [bms_union](bms_union.md)
  - [bms_intersect](bms_intersect.md)
  - [bms_difference](bms_difference.md)
  - [bms_add_members](bms_add_members.md)
  - [_copyBitmapset](../c/_copyBitmapset.md)
  - [build_index_paths](build_index_paths.md)
  - [join_is_legal](../j/join_is_legal.md)
  - [remove_rel_from_query](../r/remove_rel_from_query.md)
  - [make_outerjoininfo](../m/make_outerjoininfo.md)
  - [finalize_plan](../f/finalize_plan.md)
  - [build_join_rel](build_join_rel.md)
  - [RelationGetIndexAttrBitmap](../R/RelationGetIndexAttrBitmap.md)

## Notes and Other Information
- Returns NULL for NULL input, maintaining the empty set representation
- Uses Assert to validate input in debug builds via bms_is_valid_set
- Allocates memory using palloc, so the returned set must be freed with bms_free or pfree
- Performs a complete deep copy using memcpy for efficiency
- Widely used throughout PostgreSQL for creating independent copies of bitmaps
- Essential for operations that need to preserve original sets while creating modified versions
- The copied set is completely independent and can be modified without affecting the original

## Simplified Source

```c
Bitmapset *bms_copy(const Bitmapset *a)
{
    Bitmapset *result;
    size_t size;

    // Validate input in debug builds
    Assert(bms_is_valid_set(a));

    // Handle NULL input (empty set)
    if (a == NULL)
        return NULL;

    // Calculate total size of the bitmap structure
    size = BITMAPSET_SIZE(a->nwords);

    // Allocate new memory and copy entire structure
    result = (Bitmapset *) palloc(size);
    memcpy(result, a, size);

    return result;
}
```

This function creates a complete deep copy of a Bitmapset. It handles the NULL case (empty set) and uses memcpy for efficient copying of the entire bitmap structure including all words and metadata.