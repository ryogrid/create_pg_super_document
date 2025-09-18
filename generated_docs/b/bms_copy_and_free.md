# bms_copy_and_free

## Location
src/backend/nodes/bitmapset.c: 109 - 121

## Overview
A utility function that creates a copy of a Bitmapset and frees the original, used specifically in REALLOCATE_BITMAPSETS builds for memory management optimization.

## Definition
static Bitmapset *bms_copy_and_free(Bitmapset *a)

## Detailed Description
bms_copy_and_free is a specialized function designed for builds where REALLOCATE_BITMAPSETS is defined. It provides an efficient way to create a fresh copy of a Bitmapset while immediately freeing the original set. This pattern is useful for memory management in scenarios where the original set is no longer needed after copying.

The function serves as an atomic operation that combines copying and freeing, ensuring that memory is properly managed while maintaining the data integrity of the set. It is particularly valuable in contexts where sets need to be transformed or processed and the original can be discarded.

## Parameters / Member Variables
- `a`: Pointer to the Bitmapset to copy and free. The original set will be freed after copying.

## Dependencies
- Functions called/Symbols referenced:
  - bms_copy
  - bms_free
- Called from (representative examples):
  - bms_add_member
  - bms_del_member
  - bms_add_members
  - bms_replace_members
  - bms_add_range
  - bms_int_members
  - bms_del_members
  - bms_join

## Notes and Other Information
- This is a static function, only available within the bitmapset.c file
- Only required and compiled in REALLOCATE_BITMAPSETS builds
- Callers with multiple set parameters must be careful when using this function, as other parameters may point to the same set
- The recommended usage pattern is to call this function just before returning the resulting set
- Provides a clean way to combine copy and free operations in a single atomic step
- Used extensively in functions that modify sets and need to return fresh copies while freeing originals