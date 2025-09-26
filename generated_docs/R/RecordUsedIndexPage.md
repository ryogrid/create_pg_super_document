# RecordUsedIndexPage

## Location
src/backend/storage/freespace/indexfsm.c: 62 - 70

## Overview
RecordUsedIndexPage marks a page as completely used (no free space) in the Free Space Map (FSM).

## Definition
```c
void RecordUsedIndexPage(Relation rel, BlockNumber usedBlock)
```

## Detailed Description
RecordUsedIndexPage is a wrapper function that registers a page as completely used in the Free Space Map by recording it with zero free space. This function serves as the counterpart to RecordFreeIndexPage and is essential for maintaining accurate free space tracking.

The function calls RecordPageWithFreeSpace with a free space value of 0, indicating that the page is fully utilized and should not be considered for allocation until it has been freed or has accumulated sufficient free space through deletions.

This function is typically called immediately after a free page is allocated (as seen in GetFreeIndexPage) to prevent the same page from being allocated multiple times concurrently. It plays a crucial role in the FSM's consistency and helps avoid race conditions in multi-process environments.

## Parameters / Member Variables
- `rel`: The Relation structure representing the index containing the page to be marked as used
- `usedBlock`: The BlockNumber of the page being marked as used in the FSM

## Dependencies
- Functions called/Symbols referenced:
  - RecordPageWithFreeSpace (records the page with zero free space in FSM)
- Called from (representative examples):
  - GetFreeIndexPage (marks newly allocated pages as used)

## Notes and Other Information
- Marks pages with zero free space to indicate full utilization
- Essential for preventing concurrent allocation of the same page
- Simple wrapper around RecordPageWithFreeSpace with zero free space parameter
- Maintains FSM consistency during page allocation operations
- Used primarily for immediate marking of newly allocated pages as unavailable