# slist_node

## Location
src/include/lib/ilist.h: 223 - 224

## Overview
A basic node structure for singly linked lists that can be embedded in other structures to enable list membership.

## Definition


## Detailed Description
The  structure serves as the fundamental building block for PostgreSQL's singly linked list implementation. It is designed to be embedded within other structures that need to participate in singly linked lists. This intrusive list design allows for efficient memory usage and eliminates the need for separate allocation of list nodes.

The structure contains only a single pointer to the next node, making it a minimal overhead addition to any structure that needs list functionality. This design is commonly used throughout PostgreSQL for managing collections of objects where insertion and deletion performance is important.

## Parameters / Member Variables
- : Pointer to the next slist_node in the linked list, or NULL if this is the last node

## Dependencies
- Functions called/Symbols referenced:
  - (none - this is a basic data structure)
- Called from (representative examples):
  - [slist_head](slist_head.md) (as the node type for list management)
  - [slist_iter](slist_iter.md) (for list iteration)
  - [slist_mutable_iter](slist_mutable_iter.md) (for mutable list iteration)
  - Various PostgreSQL subsystems that embed this in their structures

## Notes and Other Information
- This is an intrusive list design - the list node is embedded directly in the data structure rather than having separate node allocations
- Used extensively throughout PostgreSQL for performance-critical list operations
- The singly linked nature means forward-only traversal, but provides better cache locality and lower memory overhead compared to doubly linked lists
- Common usage pattern involves embedding this structure in larger structures and using container macros to access the containing structure
- Part of PostgreSQL's internal list infrastructure defined in src/include/lib/ilist.h