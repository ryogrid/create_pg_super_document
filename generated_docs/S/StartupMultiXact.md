# StartupMultiXact

## Location
[src/backend/access/transam/multixact.c:2145-2169](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L2145-L2169)

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
  - [MultiXactIdToOffsetPage](../M/MultiXactIdToOffsetPage.md)
  - [MXOffsetToMemberPage](../M/MXOffsetToMemberPage.md)
  - [pg_atomic_write_u64](../p/pg_atomic_write_u64.md)
- Global variables accessed:
  - MultiXactState
  - MultiXactOffsetCtl
  - MultiXactMemberCtl
- Data types used:
  - MultiXactId
  - MultiXactOffset
- Called from:
  - [StartupXLOG](StartupXLOG.md)

## Notes and Other Information
- Function must be called exactly once during system startup
- Runs after basic MultiXact state is established but before WAL replay
- Uses atomic operations to ensure thread-safe updates to shared memory
- Critical for proper SLRU page management in the MultiXact subsystem
- Part of the startup sequence that prepares PostgreSQL for normal operation
- Does not perform any disk I/O, only updates in-memory state
- Essential for maintaining consistency between MultiXact ID/offset mappings and their storage pages

## Simplified Source

```c
// Simplified version of StartupMultiXact
void StartupMultiXact(void) {
    // Get the current MultiXact state values
    MultiXactId multi = MultiXactState->nextMXact;
    MultiXactOffset offset = MultiXactState->nextOffset;
    int64 pageno;

    // Initialize offset log's latest page number
    // Convert MultiXact ID to its offset page number
    pageno = MultiXactIdToOffsetPage(multi);
    pg_atomic_write_u64(&MultiXactOffsetCtl->shared->latest_page_number, pageno);

    // Initialize member log's latest page number
    // Convert offset to its member page number
    pageno = MXOffsetToMemberPage(offset);
    pg_atomic_write_u64(&MultiXactMemberCtl->shared->latest_page_number, pageno);
}
```

Key simplifications made:
- Added clear comments explaining each operation
- Preserved the essential two-step initialization process
- Kept atomic operations as they're critical for thread safety
- Maintained the local variable declarations for clarity
- Explained the purpose of each page number calculation