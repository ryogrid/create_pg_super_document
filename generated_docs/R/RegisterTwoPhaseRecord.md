# RegisterTwoPhaseRecord

## Location
[src/backend/access/transam/twophase.c:1264-1286](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L1264-L1286)

## Overview
RegisterTwoPhaseRecord is a function that registers a two-phase commit record to be written to the state file during transaction preparation. It provides a standardized interface for various PostgreSQL subsystems to store their state information as part of the two-phase commit protocol.

## Definition
void RegisterTwoPhaseRecord(TwoPhaseRmgrId rmid, uint16 info, const void *data, uint32 len)

## Detailed Description
RegisterTwoPhaseRecord is a core function in PostgreSQL's two-phase commit implementation that allows different resource managers to register their state data during the PREPARE phase of a distributed transaction. When called, it creates a TwoPhaseRecordOnDisk header structure containing the resource manager ID, info flags, and data length, then appends both the header and the actual data to the two-phase state records using save_state_data.

The function serves as a bridge between various PostgreSQL subsystems (locks, multixacts, predicate locks, statistics) and the two-phase commit storage mechanism. Each subsystem can use this function to ensure their critical state information is preserved and can be restored during transaction recovery or commit/rollback operations.

## Parameters / Member Variables
- `rmid`: Resource manager identifier (TwoPhaseRmgrId) that specifies which subsystem is registering the record
- `info`: 16-bit flags field containing additional information about the record type or processing hints
- `data`: Pointer to the actual data to be stored; can be NULL if len is 0
- `len`: Length in bytes of the data to be stored; if 0, only the header is written

## Dependencies
- Functions called/Symbols referenced:
  - TwoPhaseRmgrId (type for resource manager identification)
  - [TwoPhaseRecordOnDisk](../T/TwoPhaseRecordOnDisk.md) (header structure for each record)
  - [save_state_data](../s/save_state_data.md) (appends data to two-phase state records)
- Called from (representative examples):
  - [AtPrepare_MultiXact](../A/AtPrepare_MultiXact.md) (multixact subsystem preparation)
  - [AtPrepare_Locks](../A/AtPrepare_Locks.md) (lock manager preparation)
  - [AtPrepare_PredicateLocks](../A/AtPrepare_PredicateLocks.md) (predicate lock preparation)
  - [AtPrepare_PgStat_Relations](../A/AtPrepare_PgStat_Relations.md) (statistics subsystem preparation)
  - [EndPrepare](../E/EndPrepare.md) (main preparation coordination)

## Notes and Other Information
- This function is called during the PREPARE phase of two-phase commit transactions
- The registered records are later written to disk as part of the transaction's state file
- Each call creates one record with a header followed by optional data payload
- The function handles both cases where data is provided (len > 0) and header-only records (len = 0)
- Resource managers must use unique rmid values to avoid conflicts during recovery
- The records registered through this function are essential for proper transaction recovery and cleanup

## Simplified Source

```c
// Simplified version of RegisterTwoPhaseRecord
void RegisterTwoPhaseRecord(TwoPhaseRmgrId rmid, uint16 info, const void *data, uint32 len) {
    TwoPhaseRecordOnDisk record;

    // Create record header with resource manager info
    record.rmid = rmid;
    record.info = info;
    record.len = len;

    // Save the record header
    save_state_data(&record, sizeof(TwoPhaseRecordOnDisk));

    // Save the data payload if present
    if (len > 0)
        save_state_data(data, len);
}
```

Key simplifications made:
- Removed detailed comments while preserving essential logic
- Simplified variable declarations
- Highlighted the two-step process: header then data
- Preserved the conditional data saving logic
- Maintained the interface for resource manager registration