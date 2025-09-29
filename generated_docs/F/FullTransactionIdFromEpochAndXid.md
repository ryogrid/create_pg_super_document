# FullTransactionIdFromEpochAndXid

## Location
[src/include/access/transam.h:71-80](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/transam.h#L71-L80)

## Overview
Constructs a FullTransactionId by combining a 32-bit epoch with a 32-bit transaction ID (xid) to form a 64-bit full transaction identifier.

## Definition

```c
static inline FullTransactionId
FullTransactionIdFromEpochAndXid(uint32 epoch, TransactionId xid)
```
## Detailed Description
This inline function creates a FullTransactionId value by combining an epoch and transaction ID. The epoch represents the high 32 bits and the transaction ID represents the low 32 bits of the resulting 64-bit value. This is fundamental for PostgreSQL's transaction ID wraparound handling, as it extends the 32-bit transaction ID space by adding an epoch counter that increments when the transaction ID space wraps around.

The function performs a simple bit manipulation: it shifts the epoch left by 32 bits and ORs it with the transaction ID to create the full 64-bit identifier.

## Parameters / Member Variables
- : A 32-bit unsigned integer representing the transaction ID epoch (high-order bits)
- : A 32-bit TransactionId representing the transaction identifier (low-order bits)

## Dependencies
- Functions called/Symbols referenced:
  - [FullTransactionId](FullTransactionId.md) (struct type)
- Called from (representative examples):
  - [GetNewTransactionId](../G/GetNewTransactionId.md)
  - [AdvanceNextFullTransactionIdPastXid](../A/AdvanceNextFullTransactionIdPastXid.md)
  - [BootStrapXLOG](../B/BootStrapXLOG.md)
  - [GistPageGetDeleteXid](../G/GistPageGetDeleteXid.md)
  - InvalidFullTransactionId
  - FirstNormalFullTransactionId

## Notes and Other Information
- This is a static inline function defined in the header file for performance
- Essential for transaction ID wraparound prevention in PostgreSQL
- The epoch is incremented when the 32-bit transaction ID space wraps around
- Used throughout the system to create full transaction identifiers from their component parts

## Simplified Source

```c
static inline FullTransactionId
FullTransactionIdFromEpochAndXid(uint32 epoch, TransactionId xid)
{
    FullTransactionId result;

    // Combine epoch (high 32 bits) and xid (low 32 bits) into 64-bit value
    result.value = ((uint64) epoch) << 32 | xid;

    return result;
}
```