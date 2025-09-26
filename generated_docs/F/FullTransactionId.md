# FullTransactionId

## Location
src/include/access/transam.h: 65 - 68

## Overview
A 64-bit wrapped structure that contains both an epoch and a TransactionId to extend the range of transaction IDs beyond the 32-bit TransactionId limit.

## Definition

```c
typedef struct FullTransactionId
{
	uint64		value;
} FullTransactionId;
```
## Detailed Description
FullTransactionId is a fundamental data structure in PostgreSQL's transaction management system that addresses the limitation of 32-bit TransactionIds. The structure wraps a 64-bit value where the upper 32 bits represent an epoch (generation) and the lower 32 bits contain the actual TransactionId. This design prevents transaction ID wraparound issues while maintaining backward compatibility.

The structure is intentionally wrapped in a struct rather than using a raw uint64 to prevent implicit conversions to/from TransactionId, ensuring type safety. The 64-bit value is composed as: .

Not all FullTransactionId values represent valid normal XIDs - some may be special values like InvalidTransactionId or bootstrap transaction IDs.

## Parameters / Member Variables
- : A 64-bit unsigned integer where bits 63-32 contain the epoch (generation counter) and bits 31-0 contain the 32-bit TransactionId

## Dependencies
- Functions called/Symbols referenced:
  - InvalidTransactionId (for InvalidFullTransactionId constant)
  - TransactionId (underlying 32-bit transaction ID type)

- Called from (representative examples):
  - GetNewTransactionId (transaction ID generation)
  - GetCurrentFullTransactionId (current transaction access)
  - GlobalVisTestIsRemovableFullXid (visibility testing)
  - FullTransactionIdFromEpochAndXid (construction from components)
  - U64FromFullTransactionId (value extraction)

## Notes and Other Information
- The epoch increments when TransactionId wraps around from MaxTransactionId to FirstNormalTransactionId
- InvalidFullTransactionId is defined as FullTransactionIdFromEpochAndXid(0, InvalidTransactionId)
- Used extensively in visibility checking, snapshot management, and transaction state tracking
- Critical for preventing transaction ID wraparound problems in long-running PostgreSQL instances
- The structure enables PostgreSQL to handle more than 4 billion transactions without wraparound issues