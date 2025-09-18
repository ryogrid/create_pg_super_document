# xl_hash_vacuum_one_page

## Location
src/include/access/hash_xlog.h: 251 - 260

## Overview
A PostgreSQL WAL (Write-Ahead Log) record structure that stores information needed for hash index tuple deletion and meta page updates during vacuum operations on a single page.

## Definition
```c
typedef struct xl_hash_vacuum_one_page
{
    TransactionId snapshotConflictHorizon;
    uint16        ntuples;
    bool          isCatalogRel;    /* to handle recovery conflict during logical
                                   * decoding on standby */

    /* TARGET OFFSET NUMBERS */
    OffsetNumber offsets[FLEXIBLE_ARRAY_MEMBER];
} xl_hash_vacuum_one_page;
```

## Detailed Description
This structure defines the WAL record format for `XLOG_HASH_VACUUM_ONE_PAGE` operations in PostgreSQL hash indexes. It contains the necessary information to replay vacuum operations that remove dead tuples from a hash index page during crash recovery or standby replay. The structure supports variable-length arrays to accommodate different numbers of tuple offsets being vacuumed in a single operation.

The record is used when hash index pages are cleaned up during vacuum operations, specifically when dead tuples need to be removed from bucket pages. It includes transaction conflict information for proper handling during logical replication scenarios.

## Parameters / Member Variables
- `snapshotConflictHorizon`: Transaction ID used to determine snapshot conflicts during recovery, ensuring proper visibility rules during standby replay
- `ntuples`: Number of tuples being deleted from the page (corresponds to the number of elements in the offsets array)
- `isCatalogRel`: Boolean flag indicating whether this operation is on a catalog relation, used to handle recovery conflicts during logical decoding on standby servers
- `offsets`: Flexible array member containing the offset numbers of tuples to be deleted from the page

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER
  - TransactionId (PostgreSQL transaction identifier type)
  - OffsetNumber (PostgreSQL tuple offset type)

- Called from (representative examples):
  - hash_xlog_vacuum_one_page (WAL replay function)
  - _hash_vacuum_one_page (hash index vacuum implementation)
  - hash_desc (WAL record description function)
  - SizeOfHashVacuumOnePage (macro for calculating structure size)

## Notes and Other Information
- This structure is specifically designed for the `XLOG_HASH_VACUUM_ONE_PAGE` (0xC0) WAL record type
- The structure uses `FLEXIBLE_ARRAY_MEMBER` for the offsets array, allowing it to accommodate variable numbers of tuple deletions in a single WAL record
- During WAL replay, this record affects two backup blocks: the bucket page (Backup Blk 0) and the meta page (Backup Blk 1)
- The `SizeOfHashVacuumOnePage` macro calculates the base size of this structure excluding the flexible array member
- The `isCatalogRel` field is crucial for logical replication scenarios where catalog changes need special handling during recovery conflicts
- This is part of PostgreSQL hash index maintenance operations and is essential for maintaining index consistency during crash recovery