# AllocSetFreeList

## Location
src/backend/utils/mmgr/aset.c: 250 - 254

## Overview
AllocSetFreeList is a structure used to maintain free lists of AllocSetContext objects for memory context reuse optimization in PostgreSQL's allocation set memory manager.

## Definition


## Detailed Description
AllocSetFreeList is a core data structure in PostgreSQL's allocation set memory management system that implements a freelist optimization for memory context reuse. Instead of destroying and recreating AllocSetContext objects, this structure maintains linked lists of previously allocated but currently unused contexts that can be recycled for future allocations.

The system maintains two static freelist arrays () - one for default parameters and another for small parameters. This optimization reduces the overhead of memory context creation and destruction, which is particularly beneficial in scenarios with frequent context allocation and deallocation patterns.

When an AllocSetContext is deleted via , if it qualifies for freelisting, it gets added to the appropriate freelist instead of being immediately destroyed. Later, when  needs a new context, it first checks the freelist for a suitable recycled context before allocating a new one.

## Parameters / Member Variables
- : An integer tracking the current number of free contexts in this particular freelist
- : A pointer to the first AllocSetContext in the linked list of free contexts, serving as the list header

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContext (referenced as pointer type for the linked list)
- Called from (representative examples):
  - AllocSetContextCreateInternal (for recycling contexts from freelist)
  - AllocSetDelete (for adding contexts to freelist)

## Notes and Other Information
- The system uses a maximum limit of  (100) to prevent unlimited growth of the freelists
- There are exactly two freelist instances: one for default allocation parameters and one for small allocation parameters
- The freelist mechanism is an important optimization that reduces memory allocation overhead in PostgreSQL's memory management system
- Contexts in the freelist are maintained in a singly-linked list using the  field of the context header
- This structure is part of the internal implementation of PostgreSQL's allocation set memory manager (aset.c) and is not exposed to external modules