# ResourceOwnerCreate

## Location
src/backend/utils/resowner/resowner.c: 413 - 441

## Overview
Creates a new empty ResourceOwner object and establishes parent-child relationships in the resource ownership hierarchy.

## Definition

```c
struct ResourceOwnerData));
```
## Detailed Description
ResourceOwnerCreate allocates and initializes a new ResourceOwner structure in TopMemoryContext. The function establishes a hierarchical relationship where the new resource owner can have a parent, and if a parent is provided, the new owner is added to the parent's list of child owners. This hierarchy is crucial for PostgreSQL's resource management system, allowing proper cleanup and tracking of resources across different execution contexts like transactions, subtransactions, and portals.

All ResourceOwner objects are allocated in TopMemoryContext to ensure they persist until explicitly freed, as they may need to outlive the memory contexts they manage.

## Parameters / Member Variables
- : The parent ResourceOwner in the hierarchy, or NULL for top-level owners
- : A descriptive name for the resource owner, used primarily for debugging and error reporting

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextAllocZero (for allocating the ResourceOwner structure)
  - ResourceOwnerData (the structure type being allocated)
- Called from (representative examples):
  - AtStart_ResourceOwner (transaction start)
  - AtSubStart_ResourceOwner (subtransaction start)  
  - CreatePortal (portal creation)
  - perform_base_backup (backup operations)
  - CreateAuxProcessResourceOwner (auxiliary process setup)

## Notes and Other Information
- The function always allocates in TopMemoryContext to ensure ResourceOwner objects persist independently of other memory contexts
- Parent-child relationships are maintained through a singly-linked list where each parent tracks its first child, and children are linked via nextchild pointers
- The name parameter is stored as-is without duplication, so callers should ensure the name string remains valid for the lifetime of the ResourceOwner
- This function is fundamental to PostgreSQL's resource management system and is called during critical operations like transaction management and portal creation