# out_gistxlogPageReuse

## Location
src/backend/access/rmgrdesc/gistdesc.c: 26 - 36

## Overview
A static function that formats and outputs detailed information about GiST page reuse WAL records for debugging and hot standby recovery purposes.

## Definition
```c
static void out_gistxlogPageReuse(StringInfo buf, gistxlogPageReuse *xlrec)
```

## Detailed Description
This function is responsible for formatting and outputting human-readable information about GiST page reuse operations stored in WAL records. Page reuse occurs when a previously deleted page in a GiST index is recycled for new data. This information is critical for hot standby servers to properly handle snapshot conflicts and recovery scenarios.

The function formats comprehensive details including the relation identifier, block number, snapshot conflict horizon, and catalog relation status. This information enables proper conflict resolution during hot standby recovery, particularly for logical decoding scenarios.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the formatted output will be written
- `xlrec`: Pointer to a gistxlogPageReuse structure containing:
  - `locator`: RelFileLocator identifying the specific relation (spcOid/dbOid/relNumber)
  - `block`: Block number of the page being reused
  - `snapshotConflictHorizon`: FullTransactionId indicating the transaction horizon for conflict detection
  - `isCatalogRel`: Boolean flag indicating whether this is a catalog relation (affects logical decoding recovery conflicts)

## Dependencies
- Functions called/Symbols referenced:
  - [gistxlogPageReuse](../g/gistxlogPageReuse.md) (struct type)
  - EpochFromFullTransactionId (extracts epoch from FullTransactionId)
  - XidFromFullTransactionId (extracts transaction ID from FullTransactionId)
  - appendStringInfo (StringInfo formatting function)
- Called from (representative examples):
  - [gist_desc](../g/gist_desc.md) (when processing XLOG_GIST_PAGE_REUSE records)

## Notes and Other Information
- This function provides detailed output compared to many other WAL record description functions
- The snapshot conflict horizon information is essential for hot standby conflict resolution
- The isCatalogRel flag is specifically important for logical decoding during standby recovery
- Output format: "rel spcOid/dbOid/relNumber; blk blockNum; snapshotConflictHorizon epoch:xid, isCatalogRel T/F"
- Located in src/backend/access/rmgrdesc/gistdesc.c at lines 26-36