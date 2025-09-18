# TransactionIdSetCommitTs

## Location
src/backend/access/transam/commit_ts.c: 249 - 273

## Overview
Sets the commit timestamp entry for a single transaction by directly writing to the appropriate location in the SLRU page buffer.

## Definition
```c
static void TransactionIdSetCommitTs(TransactionId xid, TimestampTz ts,
                                    RepOriginId nodeid, int slotno)
```

## Detailed Description
This function performs the most granular level of commit timestamp storage by directly writing a CommitTimestampEntry structure to the correct position within an SLRU page buffer. It calculates the entry position within the page using the TransactionIdToCTsEntry macro, creates a CommitTimestampEntry structure with the provided timestamp and node ID, and copies it to the appropriate location in the page buffer using memcpy.

The function assumes the caller has already acquired the necessary SLRU bank lock and that the target page has been loaded into the specified slot. It validates that the transaction ID is normal (not bootstrap, frozen, or invalid) before proceeding with the write operation.

## Parameters / Member Variables
- `xid`: The transaction ID to set the commit timestamp for
- `ts`: The commit timestamp value to store
- `nodeid`: The replication origin ID associated with this commit
- `slotno`: The SLRU slot number where the target page is loaded

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdToCTsEntry (macro to calculate entry offset within page)
  - TransactionIdIsNormal (validation function for transaction ID)
  - [CommitTimestampEntry](../C/CommitTimestampEntry.md) (structure type for storing timestamp data)
  - CommitTsCtl (global SLRU control structure)
  - SizeOfCommitTimestampEntry (size constant for the entry structure)
  - RepOriginId (replication origin identifier type)
- Called from (representative examples):
  - [SetXidCommitTsInPage](../S/SetXidCommitTsInPage.md) (for both main transaction and subtransactions)

## Notes and Other Information
- This is a static function, only accessible within commit_ts.c
- Requires caller to hold the correct SLRU bank lock before calling
- Uses direct memory copy operation for optimal performance
- Assumes the target page is already loaded into the specified slot
- The CommitTimestampEntry structure contains both timestamp and replication origin information
- Critical low-level function for the commit timestamp subsystem
- Location: src/backend/access/transam/commit_ts.c:249-273