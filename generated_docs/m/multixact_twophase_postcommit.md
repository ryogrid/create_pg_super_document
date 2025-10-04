# multixact_twophase_postcommit

## Location
[src/backend/access/transam/multixact.c:1912-1926](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L1912-L1926)

## Overview
This function handles multixact cleanup during the post-commit phase of two-phase commit transactions, similar to AtEOXact_MultiXact but specifically for COMMIT PREPARED operations.

## Definition
```c
void multixact_twophase_postcommit(TransactionId xid, uint16 info, void *recdata, uint32 len)
```

## Detailed Description
This function is part of PostgreSQL's two-phase commit protocol handling for multixact operations. When a prepared transaction is committed, this function ensures proper cleanup of multixact state by resetting the oldest member multixact ID tracking for the dummy process number associated with the transaction. It validates that the recovery data length matches the expected size of a MultiXactId and then invalidates the OldestMemberMXactId entry for the corresponding process.

## Parameters / Member Variables
- `xid`: Transaction ID of the prepared transaction being committed
- `info`: Additional information flags (not used in current implementation)
- `recdata`: Recovery data containing the MultiXactId
- `len`: Length of the recovery data, expected to be sizeof(MultiXactId)

## Dependencies
- Functions called/Symbols referenced:
  - [TwoPhaseGetDummyProcNumber](../T/TwoPhaseGetDummyProcNumber.md)
  - ProcNumber (type)
  - MultiXactId (type)
  - InvalidMultiXactId
- Called from (representative examples):
  - [multixact_twophase_postabort](multixact_twophase_postabort.md)
  - Referenced in SizeOfMultiXactTruncate

## Notes and Other Information
- This function includes an assertion to ensure the recovery data length exactly matches sizeof(MultiXactId)
- The function works with dummy process numbers that represent prepared transactions
- Part of the broader two-phase commit infrastructure in PostgreSQL
- Located in src/backend/access/transam/multixact.c:1912-1926

## Simplified Source

```c
void
multixact_twophase_postcommit(TransactionId xid, uint16 info,
                             void *recdata, uint32 len)
{
    ProcNumber dummyProcNumber = TwoPhaseGetDummyProcNumber(xid, true);

    // Validate recovery data size
    Assert(len == sizeof(MultiXactId));

    // Clear the oldest member tracking for this committed transaction
    OldestMemberMXactId[dummyProcNumber] = InvalidMultiXactId;
}
```