# RecordNewMultiXact

## Location
[src/backend/access/transam/multixact.c:910-1025](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L910-L1025)

## Overview
Writes information about a new MultiXact into the SLRU offset and member files, handling both normal operation and WAL replay scenarios.

## Definition
```c
static void RecordNewMultiXact(MultiXactId multi, MultiXactOffset offset, int nmembers, MultiXactMember *members)
```

## Detailed Description
RecordNewMultiXact is a static function responsible for the low-level storage of MultiXact data into SLRU (Simple LRU) files. It writes the MultiXact offset information to the offsets file and the member transaction details to the members file. The function is designed to handle bank-based locking for concurrent access and properly manages page buffers for efficient I/O operations.

The function first records the offset in the MultiXactOffsetCtl SLRU, then iterates through all members to store their transaction IDs and status flags in the MultiXactMemberCtl SLRU. It optimizes performance by acquiring locks only when switching between different SLRU banks and uses condition variables to notify waiters when offset information becomes available.

## Parameters / Member Variables
- `multi`: The MultiXactId being recorded
- `offset`: The starting offset in the members file where this MultiXact's members are stored
- `nmembers`: Number of transaction members in this MultiXact
- `members`: Array of MultiXactMember structures containing transaction IDs and their lock modes

## Dependencies
- Functions called/Symbols referenced:
  - [MultiXactIdToOffsetPage](../M/MultiXactIdToOffsetPage.md), MultiXactIdToOffsetEntry (page/entry calculation)
  - [SimpleLruGetBankLock](../S/SimpleLruGetBankLock.md) (SLRU bank locking)
  - [SimpleLruReadPage](../S/SimpleLruReadPage.md) (SLRU page reading)
  - LWLockAcquire, LWLockRelease (locking primitives)
  - ConditionVariableBroadcast (notification mechanism)
  - [MXOffsetToMemberPage](../M/MXOffsetToMemberPage.md), MXOffsetToMemberOffset (member page/offset calculation)
  - [MXOffsetToFlagsOffset](../M/MXOffsetToFlagsOffset.md), MXOffsetToFlagsBitShift (flags manipulation)
  - MultiXactOffsetCtl, MultiXactMemberCtl (SLRU control structures)
- Called from (representative examples):
  - [MultiXactIdCreateFromMembers](../M/MultiXactIdCreateFromMembers.md) (during normal MultiXact creation)
  - [multixact_redo](../m/multixact_redo.md) (during WAL replay operations)

## Notes and Other Information
- Function is marked static and used internally within the MultiXact subsystem
- Handles bank-based locking to optimize concurrent access to SLRU files
- Properly manages page dirty flags to ensure data persistence
- Uses condition variables to wake up processes waiting for offset information
- Supports both normal operation and crash recovery through WAL replay
- Efficiently handles multi-page operations by minimizing lock acquisitions
- Stores member status flags using bit manipulation for space efficiency