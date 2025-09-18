# StartupMultiXact

## Location
src/backend/access/transam/multixact.c: 2145 - 2169

## Overview
StartupMultiXact initializes the MultiXact subsystem's shared memory state during postmaster or standalone-backend startup, setting up the latest page numbers for both offset and member logs.

## Definition
```c
void StartupMultiXact(void)
```

## Detailed Description
This function must be called exactly ONCE during PostgreSQL startup (either postmaster or standalone-backend mode). It runs after StartupXLOG has established the next MultiXact ID and offset values through calls to MultiXactSetNextMXact and/or MultiXactAdvanceNextMXact, and has determined the oldest MultiXact information from pg_control, but before WAL replay begins.

The function's primary responsibility is to initialize the shared memory structures that track the latest page numbers for both the offset and member logs. This involves:

1. Converting the next MultiXact ID to its corresponding offset page number and storing it atomically in the offset control structure
2. Converting the next offset value to its corresponding member page number and storing it atomically in the member control structure

These operations ensure that both SLRU (Simple LRU) subsystems have correct knowledge of their current position, which is essential for proper page management during subsequent operations.

## Parameters / Member Variables
This function takes no parameters and operates on global MultiXact state.

## Dependencies
- Functions called/Symbols referenced:
  - MultiXactIdToOffsetPage
  - MXOffsetToMemberPage
  - pg_atomic_write_u64
- Global variables accessed:
  - MultiXactState
  - MultiXactOffsetCtl
  - MultiXactMemberCtl
- Data types used:
  - MultiXactId
  - MultiXactOffset
- Called from:
  - StartupXLOG

## Notes and Other Information
- Function must be called exactly once during system startup
- Runs after basic MultiXact state is established but before WAL replay
- Uses atomic operations to ensure thread-safe updates to shared memory
- Critical for proper SLRU page management in the MultiXact subsystem
- Part of the startup sequence that prepares PostgreSQL for normal operation
- Does not perform any disk I/O, only updates in-memory state
- Essential for maintaining consistency between MultiXact ID/offset mappings and their storage pages