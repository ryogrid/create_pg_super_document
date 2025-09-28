# ResourceOwnerGetParent

## Location
[src/backend/utils/resowner/resowner.c:888-896](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/resowner/resowner.c#L888-L896)

## Overview
A simple accessor function that retrieves the parent resource owner of a given resource owner in the hierarchical ownership tree.

## Definition

```c
ResourceOwner
ResourceOwnerGetParent(ResourceOwner owner)
```
## Detailed Description
ResourceOwnerGetParent is a straightforward accessor function that provides read-only access to the parent-child relationship within the resource owner hierarchy. Resource owners in PostgreSQL are organized in a tree structure where child owners inherit certain properties and behaviors from their parents.

This function simply returns the parent pointer from the resource owner structure, returning NULL if the owner is a top-level resource owner (i.e., has no parent). This information is essential for traversing the resource owner hierarchy and understanding the ownership relationships between different transaction contexts.

The function is primarily used by the lock manager and other subsystems that need to understand the resource ownership hierarchy for proper resource management and transfer operations.

## Parameters / Member Variables
- : The ResourceOwner whose parent is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwner](ResourceOwner.md) (structure type only)
- Called from (representative examples):
  - [LockReassignCurrentOwner](../L/LockReassignCurrentOwner.md) (lock management during subtransaction commit)

## Notes and Other Information
- Returns NULL for top-level resource owners that have no parent
- Simple accessor function with no side effects or validation
- Essential for understanding resource owner hierarchy relationships
- Used primarily by lock management subsystem for resource transfer operations
- Part of the public resource owner API for hierarchy navigation
- Critical for proper subtransaction resource management where locks need to be transferred to parent contexts

## Simplified Source

```c
// Simplified version of ResourceOwnerGetParent
ResourceOwner ResourceOwnerGetParent(ResourceOwner owner) {
    // Return the parent resource owner (NULL if top-level)
    return owner->parent;
}
```

Key simplifications made:
- Function is already very simple, so minimal changes needed
- Added clear comment explaining return value
- Focused on single responsibility: returning parent pointer