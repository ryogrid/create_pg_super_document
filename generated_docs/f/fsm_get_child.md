# fsm_get_child

## Location
[src/backend/storage/freespace/freespace.c:535-553](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/freespace/freespace.c#L535-L553)

## Overview
Computes the logical address of a child page in the Free Space Map (FSM) tree structure given a parent page's logical address and a slot number.

## Definition

```c
static FSMAddress
fsm_get_child(FSMAddress parent, uint16 slot)
```
## Detailed Description
This function performs address calculation for navigating down the FSM tree hierarchy. The FSM is organized as a tree where each internal page contains slots that point to child pages at the next level down. Given a parent page's FSMAddress and a specific slot number within that parent page, this function calculates the corresponding child page's FSMAddress.

The calculation involves:
1. Decrementing the level by 1 (moving one level down in the tree)
2. Computing the child's logical page number using the formula: 

This addressing scheme ensures that child pages are laid out contiguously in logical address space, with each parent page's children occupying a contiguous range of logical page numbers.

## Parameters / Member Variables
- : FSMAddress of the parent page containing the slot
- : Slot number within the parent page (0-based index) that points to the desired child page

## Dependencies
- Functions called/Symbols referenced:
  - FSMAddress (structure type)
  - FSM_BOTTOM_LEVEL (constant)
  - SlotsPerFSMPage (constant)
- Called from (representative examples):
  - [fsm_search](fsm_search.md)
  - [fsm_vacuum_page](fsm_vacuum_page.md)

## Notes and Other Information
- This is a static function, only accessible within the freespace.c file
- Includes an assertion to ensure the parent is not at the bottom level (FSM_BOTTOM_LEVEL)
- The function assumes that the slot number is valid for the parent page
- Critical for FSM tree traversal operations during space allocation and vacuum processing