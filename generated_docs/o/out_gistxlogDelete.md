# out_gistxlogDelete

## Location
[src/backend/access/rmgrdesc/gistdesc.c:37-44](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/gistdesc.c#L37-L44)

## Overview
A static function that formats and outputs information about GiST tuple deletion WAL records for debugging and hot standby recovery purposes.

## Definition
```c
static void out_gistxlogDelete(StringInfo buf, gistxlogDelete *xlrec)
```

## Detailed Description
This function handles the formatting and output of GiST index tuple deletion operations stored in WAL records. It provides essential information about delete operations including the number of items deleted, snapshot conflict horizon for hot standby recovery, and catalog relation status for logical decoding support.

The function outputs concise but comprehensive information that helps with debugging GiST operations and ensures proper conflict resolution during hot standby recovery scenarios.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the formatted output will be written
- `xlrec`: Pointer to a gistxlogDelete structure containing:
  - `snapshotConflictHorizon`: TransactionId used for snapshot conflict detection during hot standby recovery
  - `ntodelete`: Number of index tuples/offsets being deleted from the page
  - `isCatalogRel`: Boolean flag indicating if this is a catalog relation (important for logical decoding recovery conflicts)
  - `offsets`: Array of OffsetNumbers indicating which tuples are being deleted (not directly used in output)

## Dependencies
- Functions called/Symbols referenced:
  - [gistxlogDelete](../g/gistxlogDelete.md) (struct type)
  - [appendStringInfo](../a/appendStringInfo.md) (StringInfo formatting function)
- Called from (representative examples):
  - [gist_desc](../g/gist_desc.md) (when processing XLOG_GIST_DELETE records)

## Notes and Other Information
- Output format: "delete: snapshotConflictHorizon xid, nitems: count, isCatalogRel T/F"
- The snapshotConflictHorizon is critical for resolving conflicts during hot standby recovery
- The isCatalogRel flag helps handle recovery conflicts during logical decoding on standby servers
- This function processes deletion operations on leaf pages where index tuples are removed
- Located in src/backend/access/rmgrdesc/gistdesc.c at lines 37-44

## Simplified Source

```c
static void out_gistxlogDelete(StringInfo buf, gistxlogDelete *xlrec) {
    // Format deletion info: conflict horizon, item count, and catalog flag
    appendStringInfo(buf, "delete: snapshotConflictHorizon %u, nitems: %u, isCatalogRel %c",
                     xlrec->snapshotConflictHorizon, xlrec->ntodelete,
                     xlrec->isCatalogRel ? 'T' : 'F');
}
```