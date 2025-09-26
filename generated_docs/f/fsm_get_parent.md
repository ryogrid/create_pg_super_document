# fsm_get_parent

## Location
src/backend/storage/freespace/freespace.c: 517 - 534

## Overview
Calculates the parent FSM address and slot position for a given child page in the FSM tree hierarchy.

## Definition
```c
static FSMAddress fsm_get_parent(FSMAddress child, uint16 *slot)
```

## Detailed Description
This function navigates up the FSM tree hierarchy by calculating the parent page address for a given child page. The FSM uses a tree structure where each internal node summarizes information from its children, and this function provides the essential navigation mechanism to move up the tree.

The calculation involves incrementing the level (moving up one level in the tree) and using integer division to determine which parent page contains the child, along with modulo arithmetic to find the specific slot within that parent page that corresponds to the child.

## Parameters / Member Variables
- `child`: FSMAddress structure representing the child page whose parent is to be found
- `slot`: Output parameter that receives the slot number within the parent page where this child is referenced

## Dependencies
- Functions called/Symbols referenced:
  - FSMAddress (structure type)
  - FSM_ROOT_LEVEL
  - SlotsPerFSMPage
- Called from (representative examples):
  - FSMAddress
  - fsm_search
  - fsm_vacuum_page

## Notes and Other Information
- This is a static function internal to the freespace.c module
- Contains an Assert to ensure the child is not already at the root level (FSM_ROOT_LEVEL)
- The parent level is always child.level + 1
- Each FSM page contains SlotsPerFSMPage slots, so parent page number is child.logpageno / SlotsPerFSMPage
- The slot within the parent is child.logpageno % SlotsPerFSMPage
- Essential for FSM tree traversal operations including search and vacuum
- Used during upward propagation of free space information in the FSM tree