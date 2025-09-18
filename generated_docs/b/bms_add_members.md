# bms_add_members

## Location
src/backend/nodes/bitmapset.c: 917 - 971

## Overview
Efficiently adds all members from one bitmapset to another, similar to  but optimized to recycle the left input when possible.

## Definition


## Detailed Description
The  function performs a union operation between two bitmapsets, adding all members from set  to set . Unlike , this function is optimized for the common case where the left input can be recycled, avoiding unnecessary memory allocations. The function intelligently chooses to copy the longer set and union the shorter one into it for efficiency.

When one input is significantly larger than the other, the function copies the larger set and performs bitwise OR operations with the smaller set's words. This approach minimizes memory operations and provides better performance than creating a completely new result set.

## Parameters / Member Variables
- : Left input Bitmapset to be modified (can be NULL, which copies )
- : Right input Bitmapset to add members from (const, can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - : Validates both input bitmapset structures  
  - : Creates a copy of a bitmapset
  - : Optional copy-and-free operation (when REALLOCATE_BITMAPSETS is defined)
  - : PostgreSQL memory deallocation function
- Called from (representative examples):
  - : Heap tuple update operations
  - : Query explain plan scanning
  - : Equivalence class member addition
  - : Join clause creation in equivalence classes
  - : Index path building
  - : Bitmap AND path selection
  - : Subplan finalization with parameter handling

## Notes and Other Information
- Returns a copy of  when  is NULL
- Returns  unchanged when  is NULL (after optional copy-and-free)
- Chooses the more efficient strategy based on relative sizes of the input sets
- Frees the original  when copying  for the result
- Under  compile flag, performs additional memory safety operations
- More efficient than  for cases where left input recycling is beneficial
- Extensively used in query optimization where accumulating sets of relations or attributes is common
- Essential for building composite bitmapsets during join planning and equivalence class processing
- Maintains the property that the result contains all members from both input sets