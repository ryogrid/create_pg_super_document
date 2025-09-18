# bms_copy

## Location
src/backend/nodes/bitmapset.c: 122 - 141

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
  - bms_is_valid_set (validation in Assert)
  - BITMAPSET_SIZE (macro for calculating bitmap size)
  - palloc (memory allocation)
  - memcpy (memory copying)
- Called from (representative examples):
  - afterTriggerCopyBitmap
  - CreatePartitionPruneState
  - ExecFindMatchingSubPlans
  - bms_copy_and_free
  - bms_union
  - bms_intersect
  - bms_difference
  - bms_add_members
  - _copyBitmapset
  - build_index_paths
  - join_is_legal
  - remove_rel_from_query
  - make_outerjoininfo
  - finalize_plan
  - build_join_rel
  - RelationGetIndexAttrBitmap

## Notes and Other Information
- Returns NULL for NULL input, maintaining the empty set representation
- Uses Assert to validate input in debug builds via bms_is_valid_set
- Allocates memory using palloc, so the returned set must be freed with bms_free or pfree
- Performs a complete deep copy using memcpy for efficiency
- Widely used throughout PostgreSQL for creating independent copies of bitmaps
- Essential for operations that need to preserve original sets while creating modified versions
- The copied set is completely independent and can be modified without affecting the original