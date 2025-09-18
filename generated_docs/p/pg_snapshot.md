# pg_snapshot

## Location
src/backend/utils/adt/xid8funcs.c: 68 - 69

## Overview
The `pg_snapshot` struct represents a snapshot containing full 64-bit transaction IDs (FullTransactionId) used for transaction visibility determination and snapshot management in PostgreSQL.

## Definition
```c
typedef struct
{
    /*
     * 4-byte length hdr, should not be touched directly.
     *
     * Explicit embedding is ok as we want always correct alignment anyway.
     */
    int32       __varsz;

    uint32      nxip;           /* number of fxids in xip array */
    FullTransactionId xmin;
    FullTransactionId xmax;
    /* in-progress fxids, xmin <= xip[i] < xmax: */
    FullTransactionId xip[FLEXIBLE_ARRAY_MEMBER];
} pg_snapshot;
```

## Detailed Description
The `pg_snapshot` structure is a variable-length data type that stores snapshot information using full 64-bit transaction IDs. It is primarily used in the xid8funcs.c module to support functions that export internal transaction IDs to user level, including `pg_current_snapshot()` and related functions.

This structure represents a point-in-time view of the transaction system state, containing information about which transactions were active, committed, or yet to start at the time the snapshot was taken. Unlike the internal Snapshot structure used by PostgreSQL's MVCC system, `pg_snapshot` uses FullTransactionId values to avoid wraparound issues when exposing transaction IDs to users.

The struct uses a flexible array member design where the `xip` array contains the transaction IDs of in-progress transactions at the time the snapshot was created. The structure is designed to be stored as a PostgreSQL varlena type with proper alignment.

## Parameters / Member Variables
- `__varsz`: 4-byte length header for varlena type compatibility, should not be manipulated directly
- `nxip`: Number of transaction IDs stored in the xip array (number of in-progress transactions)
- `xmin`: Minimum transaction ID that was still active when the snapshot was taken
- `xmax`: Maximum transaction ID that had been assigned when the snapshot was taken  
- `xip[]`: Flexible array containing the full transaction IDs of transactions that were in-progress, where xmin ≤ xip[i] < xmax

## Dependencies
- Functions called/Symbols referenced:
  - FullTransactionId (struct type for 64-bit transaction IDs)
  - FLEXIBLE_ARRAY_MEMBER (macro for variable-length arrays)

- Called from (representative examples):
  - PG_SNAPSHOT_SIZE (macro for calculating structure size)
  - PG_SNAPSHOT_MAX_NXIP (macro for maximum array size)
  - sort_snapshot (function for sorting snapshot contents)
  - is_visible_fxid (function for transaction visibility checks)
  - buf_init, buf_add_txid, buf_finalize (snapshot construction functions)
  - pg_current_snapshot (function to get current snapshot)
  - pg_snapshot_in, pg_snapshot_out (I/O functions)
  - pg_snapshot_recv, pg_snapshot_send (binary I/O functions)
  - pg_visible_in_snapshot (visibility check function)
  - pg_snapshot_xmin, pg_snapshot_xmax, pg_snapshot_xip (accessor functions)

## Notes and Other Information
- The structure uses FullTransactionId instead of TransactionId to avoid 32-bit wraparound issues
- Maximum number of XIDs in the array is limited by PG_SNAPSHOT_MAX_NXIP macro
- The xip array is maintained in sorted order for efficient binary search operations
- Used primarily for user-facing transaction ID functions and snapshot export/import
- The structure layout ensures proper alignment for all platforms
- Size calculation is handled by PG_SNAPSHOT_SIZE macro to account for variable-length array
- Static assertions ensure that the maximum backends limit doesn't exceed the maximum array size