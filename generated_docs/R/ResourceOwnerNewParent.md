# ResourceOwnerNewParent

## Location
[src/backend/utils/resowner/resowner.c:897-943](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/resowner/resowner.c#L897-L943)

## Overview
Reassigns a resource owner to have a new parent, properly maintaining the hierarchical linked list structure of the resource owner tree.

## Definition

```c
void
ResourceOwnerNewParent(ResourceOwner owner,
					   ResourceOwner newparent)
```
## Detailed Description
ResourceOwnerNewParent manages the reparenting of resource owners within the hierarchical ownership tree. This function is essential for maintaining the integrity of the resource owner hierarchy when ownership relationships change, such as during subtransaction operations or when unlinking owners before deletion.

The function performs a two-phase operation:

**Phase 1 - Unlinking from old parent:**
If the owner currently has a parent, it removes the owner from the parent's child list. This involves traversing the singly-linked list of children to find and remove the owner, updating the appropriate pointers to maintain list continuity.

**Phase 2 - Linking to new parent:**
If a new parent is specified, the owner is inserted at the head of the new parent's child list. If newparent is NULL, the owner becomes a top-level resource owner with no parent.

The function maintains the singly-linked list structure used for child management, where each parent maintains a  pointer and children are linked through  pointers.

## Parameters / Member Variables
- : The ResourceOwner to be reparented
- : The new parent ResourceOwner, or NULL to make owner a top-level resource owner

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwner (structure type for ownership hierarchy)
- Called from (representative examples):
  - [AtSubCommit_Portals](../A/AtSubCommit_Portals.md) (portal management during subtransaction commit)
  - [AtSubAbort_Portals](../A/AtSubAbort_Portals.md) (portal management during subtransaction abort) 
  - [ResourceOwnerDelete](ResourceOwnerDelete.md) (unlinking before deletion to prevent corruption)

## Notes and Other Information
- Handles both reparenting to a new parent and orphaning (setting parent to NULL)
- Maintains singly-linked list integrity during parent-child relationship changes
- Includes assertion to prevent self-parenting (owner cannot be its own parent)
- Critical for subtransaction processing where resource ownership needs to be transferred
- Used during portal management when portals need to change ownership contexts
- Essential safety mechanism in ResourceOwnerDelete to prevent dangling pointers
- Supports the dynamic nature of PostgreSQL's transaction hierarchy management