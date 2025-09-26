# FullTransactionIdFromU64

## Location
[src/include/access/transam.h:81-90](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/transam.h#L81-L90)

## Overview
Creates a FullTransactionId from a 64-bit unsigned integer value by directly assigning the value to the FullTransactionId structure.

## Definition
```c
static inline FullTransactionId
FullTransactionIdFromU64(uint64 value)
```

## Detailed Description
This inline function provides a simple wrapper to create a FullTransactionId from a raw 64-bit unsigned integer. It directly assigns the provided value to the FullTransactionId's internal value field. This function is commonly used when deserializing transaction IDs from storage formats, network protocols, or when converting from other representations where the full 64-bit transaction ID is already available as a single value.

## Parameters / Member Variables
- `value`: A 64-bit unsigned integer representing the complete full transaction identifier

## Dependencies
- Functions called/Symbols referenced:
  - [FullTransactionId](FullTransactionId.md) (struct type)
- Called from (representative examples):
  - [restoreTwoPhaseData](../r/restoreTwoPhaseData.md)
  - [FullXidRelativeTo](FullXidRelativeTo.md)
  - [xid8in](../x/xid8in.md)
  - [xid8recv](../x/xid8recv.md)
  - [parse_snapshot](../p/parse_snapshot.md)
  - [pg_snapshot_recv](../p/pg_snapshot_recv.md)
  - [DatumGetFullTransactionId](../D/DatumGetFullTransactionId.md)

## Notes and Other Information
- This is a static inline function for performance
- Provides a clean interface for creating FullTransactionId from raw 64-bit values
- Commonly used in serialization/deserialization contexts
- Used when reading transaction IDs from WAL records, snapshots, or other storage formats
- Complementary to FullTransactionIdFromEpochAndXid which constructs from separate epoch and xid components