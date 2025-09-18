# find_unconnected_slot

## Location
src/fe_utils/parallel_slot.c: 159 - 178

## Overview
A static function that searches through a parallel slot array to find the first available slot that does not have an active database connection.

## Definition
```c
static int find_unconnected_slot(const ParallelSlotArray *sa)
```

## Detailed Description
This function performs a linear search through all slots in a parallel slot array to locate the first slot that is available for establishing a new database connection. A suitable slot must not be currently in use and must not have an existing database connection. This function is typically used when a new database connection needs to be established and an unconnected slot is required to host that connection. The search returns the index of the first qualifying slot, allowing for efficient slot allocation in parallel processing scenarios.

## Parameters / Member Variables
- `sa`: A const pointer to the ParallelSlotArray containing all available slots to search

## Dependencies
- Functions called/Symbols referenced:
  - [ParallelSlotArray](../P/ParallelSlotArray.md) (struct type for the slot array)
- Called from (representative examples):
  - [ParallelSlotsGetIdle](../P/ParallelSlotsGetIdle.md)
  - [ParallelSlotsAdoptConn](../P/ParallelSlotsAdoptConn.md)

## Notes and Other Information
- This is a static function, only accessible within the parallel_slot.c file
- Returns -1 when no unconnected slot is found, otherwise returns the zero-based index of the first unconnected slot
- Performs linear search through the slot array, returning the first match found
- The function checks two conditions: slot not in use and no existing connection
- Part of PostgreSQL's frontend utility library for managing parallel database connections
- Complementary to find_matching_idle_slot - this finds slots for new connections while the other finds slots with existing connections
- Used for connection establishment in parallel processing workflows