# descriptor_deallocate_all

## Location
src/interfaces/ecpg/ecpglib/descriptor.c: 780 - 791

## Overview
A static utility function that deallocates all descriptors in a linked list, traversing the entire list and freeing each descriptor.

## Definition
```c
static void descriptor_deallocate_all(struct descriptor *list)
```

## Detailed Description
This internal function iterates through a linked list of descriptors, deallocating each one by calling `descriptor_free()`. The function safely traverses the list by storing the next pointer before freeing each node, preventing access to freed memory. This is a cleanup utility function designed to deallocate entire descriptor lists, typically used during library shutdown or error recovery scenarios.

## Parameters / Member Variables
- `list`: Pointer to the head of the descriptor linked list to deallocate

## Dependencies
- Functions called/Symbols referenced:
  - descriptor_free: Frees memory associated with a single descriptor
  - struct descriptor: The descriptor structure type

- Called from (representative examples):
  - descriptor_destructor: Called during library cleanup/shutdown

## Notes and Other Information
- This is a static function, only accessible within the descriptor.c compilation unit
- The function handles NULL list pointers safely (the while loop simply won't execute)
- Memory is freed in a forward traversal manner, with each node being freed after storing its next pointer
- Used for cleanup operations, particularly during library termination or when deallocating entire descriptor collections
- Part of the internal implementation of the ECPG descriptor management system
- The function ensures no memory leaks by completely deallocating all nodes in the provided list