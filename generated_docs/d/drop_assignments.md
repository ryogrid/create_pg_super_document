# drop_assignments

## Location
src/interfaces/ecpg/preproc/descriptor.c: 33 - 45

## Overview
Deallocates and removes all assignment entries from the global assignments linked list, cleaning up memory used by variable-descriptor assignments.

## Definition


## Detailed Description
This function iterates through the global assignments linked list and deallocates all nodes and their associated memory. It's a cleanup function that ensures proper memory management by freeing both the variable name strings and the assignment structures themselves. The function continues until the assignments list is completely empty.

As a static function, it's only accessible within the descriptor.c file and serves as an internal cleanup mechanism for the ECPG preprocessor's assignment tracking system.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - free (standard library deallocation function)
  - assignment (struct type)
- Called from (representative examples):
  - output_get_descr_header
  - output_get_descr  
  - output_set_descr_header
  - output_set_descr

## Notes and Other Information
- Static function with file-local scope in descriptor.c
- Implements proper cleanup by freeing both the variable string and the assignment structure
- Called by various output functions to clean up assignments after processing descriptor operations
- Ensures no memory leaks in the assignment tracking system
- Uses standard free() rather than a custom deallocator, contrasting with the mm_alloc used in push_assignment