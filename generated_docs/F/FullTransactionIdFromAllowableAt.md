# FullTransactionIdFromAllowableAt

## Location
[src/include/access/transam.h:381-418](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/transam.h#L381-L418)

## Overview
Computes a full 64-bit transaction ID from a 32-bit transaction ID, assuming the 32-bit ID was valid within a specific transaction ID range at a given point in time.

## Definition
```c
static inline FullTransactionId FullTransactionIdFromAllowableAt(FullTransactionId nextFullXid, TransactionId xid)
```

## Detailed Description
This function reconstructs the full 64-bit transaction ID (including epoch) from a 32-bit transaction ID, given the context of what the next full transaction ID was at the time the 32-bit ID was collected. The function handles the complex logic of determining which epoch the transaction ID belongs to, considering PostgreSQL's circular 32-bit transaction ID space and the 64-bit full transaction ID space.

The function implements sophisticated logic to determine whether the 32-bit transaction ID belongs to the current epoch or the previous epoch. Since transaction IDs wrap around every 2^32 transactions, and the system must freeze old XIDs before they become too old, the function can safely assume that any given XID is from either the current epoch or at most one epoch behind.

The function includes performance optimizations with branch prediction hints, favoring the common case where the XID is from the current epoch rather than a previous epoch.

## Parameters / Member Variables
- `nextFullXid`: The full transaction ID that represents the next transaction ID that would be assigned at the reference point in time
- `xid`: The 32-bit transaction ID to convert to a full transaction ID

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdIsNormal
  - [FullTransactionIdFromEpochAndXid](FullTransactionIdFromEpochAndXid.md)
  - [TransactionIdPrecedesOrEquals](../T/TransactionIdPrecedesOrEquals.md)
  - XidFromFullTransactionId
  - EpochFromFullTransactionId
- Called from (representative examples):
  - [AdjustToFullTransactionId](../A/AdjustToFullTransactionId.md) (in two-phase commit processing)
  - [XLogRecGetFullXid](../X/XLogRecGetFullXid.md) (in WAL record processing)
  - [TransactionIdInRecentPast](../T/TransactionIdInRecentPast.md) (in XID8 utility functions)
  - [pg_current_snapshot](../p/pg_current_snapshot.md) (for snapshot introspection)

## Notes and Other Information
- This is an inline function defined in the transaction management header file
- Handles special transaction IDs by assigning them to epoch 0
- Includes assertions to verify the input XID is within the allowable range
- Uses branch prediction hints (unlikely) to optimize for the common case where XIDs are from the current epoch
- Critical for converting historical 32-bit transaction IDs to full 64-bit form when processing WAL records, snapshots, and two-phase commit state
- The function's design accounts for PostgreSQL's requirement to freeze old XIDs before they become ambiguous due to wraparound
- Essential for maintaining transaction visibility and consistency when working with mixed 32-bit and 64-bit transaction ID representations