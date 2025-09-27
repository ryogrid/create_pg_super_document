# GetReplicationHorizons

## Location
[src/backend/storage/ipc/procarray.c:2047-2068](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L2047-L2068)

## Overview
GetReplicationHorizons provides visibility horizon information specifically for hot standby feedback messages, enabling efficient replication coordination between primary and standby servers.

## Definition
```c
void
GetReplicationHorizons(TransactionId *xmin, TransactionId *catalog_xmin)
```

## Detailed Description
This function computes and returns two distinct transaction horizons used in PostgreSQL's hot standby feedback mechanism. The feedback system allows standby servers to inform the primary about which transactions they still need, preventing the primary from vacuuming away data that standbys are still using.

The function deliberately separates data and catalog horizons:
- **xmin (shared_oldest_nonremovable_raw)**: The horizon for regular data tables, excluding the influence of replication slot catalog requirements
- **catalog_xmin (slot_catalog_xmin)**: The horizon specifically for catalog tables based on replication slot needs

This separation is crucial because it allows the primary server to be more aggressive in cleaning up regular data tables while being appropriately conservative with catalog data that logical replication slots might need for decoding.

The function specifically avoids using `shared_oldest_nonremovable` because that value already incorporates the catalog horizon, which would make the feedback less granular and less efficient.

## Parameters / Member Variables
- `xmin`: Output parameter for the transaction horizon for regular data tables
- `catalog_xmin`: Output parameter for the transaction horizon for catalog tables

## Dependencies
- Functions called/Symbols referenced:
  - [ComputeXidHorizons](../C/ComputeXidHorizons.md)
  - [ComputeXidHorizonsResult](../C/ComputeXidHorizonsResult.md) (struct type)
- Called from:
  - [XLogWalRcvSendHSFeedback](../X/XLogWalRcvSendHSFeedback.md) (in walreceiver.c)

## Notes and Other Information
- Essential for hot standby feedback mechanism in streaming replication
- The separation of data and catalog horizons enables more efficient vacuum behavior on the primary
- Used exclusively in replication contexts, not for local vacuum decisions
- The "raw" horizon excludes replication slot influence to provide cleaner feedback boundaries
- Critical for maintaining consistency in replication while allowing optimal cleanup on the primary server
- The function design reflects PostgreSQL's sophisticated approach to balancing replication needs with storage efficiency

## Simplified Source

```c
// Simplified version of GetReplicationHorizons
void GetReplicationHorizons(TransactionId *xmin, TransactionId *catalog_xmin) {
    ComputeXidHorizonsResult horizons;

    // Get current transaction horizons from the system
    ComputeXidHorizons(&horizons);

    // Set data table horizon (excludes replication slot catalog influence)
    // This allows more aggressive cleanup of regular data
    *xmin = horizons.shared_oldest_nonremovable_raw;

    // Set catalog table horizon (based on replication slot requirements)
    // This is more conservative to preserve catalog data for logical replication
    *catalog_xmin = horizons.slot_catalog_xmin;
}
```

Key simplifications made:
- Added clear comments explaining the purpose of each horizon
- Clarified why the "raw" horizon is used for data tables
- Explained the conservative approach for catalog tables
- Maintained the essential two-step process: compute horizons, then extract specific values