# TransactionIdOlder

## Location
[src/include/access/transam.h:334-348](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/transam.h#L334-L348)

## Overview
Returns the chronologically older of two transaction IDs, handling invalid transaction IDs and using PostgreSQL's modular transaction ID arithmetic.

## Definition
```c
static inline TransactionId TransactionIdOlder(TransactionId a, TransactionId b)
```

## Detailed Description
This function compares two transaction IDs and returns the one that is chronologically older according to PostgreSQL's circular transaction ID ordering system. It first handles special cases where one or both transaction IDs might be invalid, returning the valid one if only one is valid. For two valid transaction IDs, it uses the TransactionIdPrecedes function to determine which one logically precedes the other in the modular arithmetic system, returning the preceding (older) transaction ID.

The function is essential for operations that need to find the earliest transaction ID among multiple candidates, such as computing transaction visibility horizons and snapshot boundaries.

## Parameters / Member Variables
- `a`: First transaction ID to compare
- `b`: Second transaction ID to compare

## Dependencies
- Functions called/Symbols referenced:
  - [TransactionIdPrecedes](TransactionIdPrecedes.md)
- Called from (representative examples):
  - [ComputeXidHorizons](../C/ComputeXidHorizons.md) (multiple calls for calculating transaction visibility horizons)
  - [GetSnapshotData](../G/GetSnapshotData.md) (for snapshot computation)

## Notes and Other Information
- This is an inline function defined in the transaction management header file
- Handles invalid transaction IDs gracefully by returning the valid one when only one is valid
- Extensively used in visibility computation and snapshot management
- The function relies on TransactionIdPrecedes for the core comparison logic, which handles PostgreSQL's circular transaction ID space correctly
- Critical for determining transaction visibility boundaries and garbage collection thresholds