# FullXidRelativeTo

## Location
[src/backend/storage/ipc/procarray.c:4320-4401](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L4320-L4401)

## Overview
Converts a 32-bit transaction ID to a 64-bit FullTransactionId by inferring the correct epoch based on its proximity to a reference full transaction ID.

## Definition
```c
static inline FullTransactionId FullXidRelativeTo(FullTransactionId rel, TransactionId xid)
```

## Detailed Description
This function safely converts a 32-bit TransactionId to a FullTransactionId by assuming the target transaction ID is within MaxTransactionId/2 (approximately 2 billion transactions) of the reference full transaction ID. The conversion is performed by calculating the signed difference between the 32-bit portions and applying it to the full 64-bit reference.

The function uses modular arithmetic to handle transaction ID wraparound correctly. It extracts the 32-bit portion from the reference FullTransactionId, calculates the signed difference with the target xid, and adds this difference to the full 64-bit reference value. This approach preserves the correct epoch information while handling wraparound scenarios.

Safety is ensured through careful usage constraints: the function can only be used when there's a guarantee that the xid is within the allowable range of the reference transaction ID. This is typically satisfied when holding a snapshot and processing table data (protected by vacuum/freezing) or when dealing with procarray data that prevents wraparound.

## Parameters / Member Variables
- `rel`: Reference FullTransactionId used to determine the correct epoch for the conversion
- `xid`: 32-bit transaction ID to convert (must be within MaxTransactionId/2 of rel)

## Dependencies
- Functions called/Symbols referenced:
  - XidFromFullTransactionId
  - AssertTransactionIdInAllowableRange
  - U64FromFullTransactionId
  - FullTransactionIdFromU64
  - FullTransactionId (type)
- Called from (representative examples):
  - xc_slow_answer_inc
  - [MaintainLatestCompletedXid](../M/MaintainLatestCompletedXid.md)
  - [MaintainLatestCompletedXidRecovery](../M/MaintainLatestCompletedXidRecovery.md)
  - [GetSnapshotData](../G/GetSnapshotData.md)
  - [GlobalVisUpdateApply](../G/GlobalVisUpdateApply.md)
  - [GlobalVisTestIsRemovableXid](../G/GlobalVisTestIsRemovableXid.md)

## Notes and Other Information
- Critical safety requirement: xid must be within MaxTransactionId/2 of the reference transaction ID
- Uses signed 32-bit arithmetic to handle wraparound correctly
- Includes assertions to validate input transaction IDs and catch common mistakes
- Static inline function for performance in frequently called code paths
- Essential component of PostgreSQL's transaction ID management system
- The conversion preserves transaction ordering relationships across epoch boundaries