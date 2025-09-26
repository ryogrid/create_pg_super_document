# FullTransactionIdNewer

## Location
[src/include/access/transam.h:360-380](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/transam.h#L360-L380)

## Overview
Returns the chronologically newer of two full transaction IDs, handling invalid transaction IDs and using 64-bit full transaction ID comparison.

## Definition
```c
static inline FullTransactionId FullTransactionIdNewer(FullTransactionId a, FullTransactionId b)
```

## Detailed Description
This function compares two full transaction IDs and returns the one that is chronologically newer according to PostgreSQL's transaction ordering system. Unlike regular TransactionId which uses 32-bit values and modular arithmetic, FullTransactionId uses 64-bit values that provide a much larger address space and avoid wraparound issues for a much longer period.

The function first handles special cases where one or both transaction IDs might be invalid, returning the valid one if only one is valid. For two valid full transaction IDs, it uses FullTransactionIdFollows to determine which one logically follows the other, returning the following (newer) transaction ID.

This function is particularly important for operations that need to track the most recent transaction activities and maintain accurate visibility information using the extended transaction ID space.

## Parameters / Member Variables
- `a`: First full transaction ID to compare
- `b`: Second full transaction ID to compare

## Dependencies
- Functions called/Symbols referenced:
  - FullTransactionIdIsValid
  - FullTransactionIdFollows
- Called from (representative examples):
  - GetSnapshotData (multiple calls for snapshot computation with full transaction IDs)
  - GlobalVisUpdateApply (for global visibility updates)

## Notes and Other Information
- This is an inline function defined in the transaction management header file
- Works with 64-bit FullTransactionId values rather than 32-bit TransactionId values
- Handles invalid full transaction IDs gracefully by returning the valid one when only one is valid
- Used primarily in snapshot management and visibility computation where extended transaction ID range is beneficial
- The function is the FullTransactionId equivalent of TransactionIdOlder, but returns the newer rather than older ID
- Essential for maintaining accurate transaction visibility in systems with high transaction rates where 32-bit transaction IDs might wrap around more frequently