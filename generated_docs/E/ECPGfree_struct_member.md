# ECPGfree_struct_member

## Location
src/interfaces/ecpg/preproc/type.c: 641 - 654

## Overview
ECPGfree_struct_member is a utility function that recursively frees a linked list of ECPGstruct_member structures, properly deallocating all associated memory including member names and types.

## Definition


## Detailed Description
This function implements a standard linked list deallocation pattern for ECPGstruct_member structures. It traverses the linked list of struct members, freeing each member's name string, type information, and the member structure itself. The function handles the recursive nature of the linked list by advancing to the next member before freeing the current one, preventing access to freed memory during traversal.

## Parameters / Member Variables
- : Pointer to the first ECPGstruct_member in the linked list to be freed. Can be NULL (function will handle gracefully)

## Dependencies
- Functions called/Symbols referenced:
  - free (standard C library function for memory deallocation)
  - ECPGstruct_member (struct definition for member list nodes)
- Called from (representative examples):
  - ECPGfree_type (when freeing struct/union types)
  - main (cleanup at program termination)

## Notes and Other Information
- Function safely handles NULL input - if rm is NULL, the while loop never executes
- Uses proper linked list traversal pattern: saves next pointer before freeing current node
- Frees three components per member: name string, type pointer, and the member structure itself
- Part of the memory management system for ECPG type structures
- This function is essential for preventing memory leaks when struct definitions are no longer needed
- The type field being freed likely points to an ECPGtype structure that should be freed separately if it's not shared