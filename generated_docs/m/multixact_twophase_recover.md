# multixact_twophase_recover

## Location
[src/backend/access/transam/multixact.c:1891-1911](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L1891-L1911)

## Overview
Recovers MultiXact state for prepared transactions during database startup or crash recovery.

## Definition
```c
void multixact_twophase_recover(TransactionId xid, uint16 info, void *recdata, uint32 len)
```

## Detailed Description
This function is part of the two-phase commit recovery mechanism and is responsible for restoring MultiXact state that was saved during the prepare phase of a two-phase commit transaction. It extracts the OldestMemberMXactId value from the recovery data and assigns it to the appropriate dummy process slot for the prepared transaction. This ensures that MultiXact visibility and cleanup semantics are properly maintained across database restarts and recovery operations.

## Parameters / Member Variables
- `xid`: The transaction ID of the prepared transaction being recovered
- `info`: Additional information about the recovery record (unused in this function)
- `recdata`: Pointer to the recovery data containing the saved MultiXactId
- `len`: Length of the recovery data (should be sizeof(MultiXactId))

## Dependencies
- Functions called/Symbols referenced:
  - [TwoPhaseGetDummyProcNumber](../T/TwoPhaseGetDummyProcNumber.md)
  - ProcNumber (type)
  - MultiXactId (type)
  - Assert (validation macro)
- Global variables modified:
  - OldestMemberMXactId[dummyProcNumber]
- Called from (representative examples):
  - Two-phase commit recovery system (registered as callback)

## Notes and Other Information
- Called during database startup or crash recovery for prepared transactions
- Validates that the recovery data length matches sizeof(MultiXactId)
- Counterpart to AtPrepare_MultiXact which saves the state
- Essential for maintaining MultiXact consistency across system restarts
- Registered as a recovery callback with the two-phase commit resource manager
- Part of PostgreSQL's crash recovery and two-phase commit infrastructure
- Located in src/backend/access/transam/multixact.c:1891-1911

## Simplified Source

```c
void
multixact_twophase_recover(TransactionId xid, uint16 info,
                          void *recdata, uint32 len)
{
    ProcNumber dummyProcNumber = TwoPhaseGetDummyProcNumber(xid, false);
    MultiXactId oldestMember;

    // Extract the oldest member XID from recovery data
    Assert(len == sizeof(MultiXactId));
    oldestMember = *((MultiXactId *) recdata);

    // Restore the oldest member tracking for this prepared transaction
    OldestMemberMXactId[dummyProcNumber] = oldestMember;
}
```