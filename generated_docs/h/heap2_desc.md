# heap2_desc

## Location
[src/backend/access/rmgrdesc/heapdesc.c:260-384](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/heapdesc.c#L260-L384)

## Overview
This function provides human-readable descriptions of heap2 WAL (Write-Ahead Logging) record types for PostgreSQL debugging and analysis purposes.

## Definition

```c
void
heap2_desc(StringInfo buf, XLogReaderState *record)
```
## Detailed Description
 is a WAL record description function that parses and formats heap2-related WAL records into readable text. It handles various heap2 operation types including:

- **PRUNE operations** (ON_ACCESS, VACUUM_SCAN, VACUUM_CLEANUP): Describes pruning operations with conflict horizons, catalog relation flags, and detailed information about redirected, dead, and unused tuples
- **VISIBLE operations**: Describes visibility map updates with snapshot conflict horizons and flags
- **MULTI_INSERT operations**: Describes bulk tuple insertions with tuple counts, flags, and offset information
- **LOCK_UPDATED operations**: Describes tuple lock updates with transaction IDs, offsets, and info bits
- **NEW_CID operations**: Describes new command ID assignments with relation and tuple identifiers

The function extracts structured data from the WAL record and formats it into a string buffer for display in PostgreSQL logs and debugging tools.

## Parameters / Member Variables
- `buf`: StringInfo buffer to append the formatted description text
- `*record`: XLogReaderState pointer containing the WAL record data to be described
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - XLogRecGetInfo
  - XLogRecHasBlockData
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md)
  - [heap_xlog_deserialize_prune_and_freeze](heap_xlog_deserialize_prune_and_freeze.md)
  - [appendStringInfo](../a/appendStringInfo.md)
  - [appendStringInfoString](../a/appendStringInfoString.md)
  - [array_desc](../a/array_desc.md)
  - [infobits_desc](../i/infobits_desc.md)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
- Called from (representative examples):
  - WAL record description infrastructure (indirectly through resource manager tables)

## Notes and Other Information
- This function is part of PostgreSQL's WAL record description system, used for debugging and log analysis
- It handles complex heap2 operations that involve multiple tuple modifications in a single WAL record
- The function uses helper functions like  to format arrays of data structures
- Different heap2 operation types require different parsing and formatting approaches
- The function is located in src/backend/access/rmgrdesc/heapdesc.c:260-384

## Simplified Source

```c
void heap2_desc(StringInfo buf, XLogReaderState *record) {
    char *rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    info &= XLOG_HEAP_OPMASK;

    // Handle PRUNE operations (ON_ACCESS, VACUUM_SCAN, VACUUM_CLEANUP)
    if (info == XLOG_HEAP2_PRUNE_ON_ACCESS ||
        info == XLOG_HEAP2_PRUNE_VACUUM_SCAN ||
        info == XLOG_HEAP2_PRUNE_VACUUM_CLEANUP) {

        xl_heap_prune *xlrec = (xl_heap_prune *) rec;

        // Show conflict horizon if present
        if (xlrec->flags & XLHP_HAS_CONFLICT_HORIZON) {
            TransactionId conflict_xid;
            memcpy(&conflict_xid, rec + SizeOfHeapPrune, sizeof(TransactionId));
            appendStringInfo(buf, "snapshotConflictHorizon: %u", conflict_xid);
        }

        // Show catalog relation flag
        appendStringInfo(buf, ", isCatalogRel: %c",
                        xlrec->flags & XLHP_IS_CATALOG_REL ? 'T' : 'F');

        // Process block data if present
        if (XLogRecHasBlockData(record, 0)) {
            // Deserialize prune and freeze data
            Size datalen;
            char *cursor = XLogRecGetBlockData(record, 0, &datalen);

            // Extract arrays of tuples affected
            int nplans, nredirected, ndead, nunused;
            xlhp_freeze_plan *plans;
            OffsetNumber *frz_offsets, *redirected, *nowdead, *nowunused;

            heap_xlog_deserialize_prune_and_freeze(cursor, xlrec->flags,
                                                  &nplans, &plans, &frz_offsets,
                                                  &nredirected, &redirected,
                                                  &ndead, &nowdead,
                                                  &nunused, &nowunused);

            // Display counts
            appendStringInfo(buf, ", nplans: %u, nredirected: %u, ndead: %u, nunused: %u",
                           nplans, nredirected, ndead, nunused);

            // Display arrays using helper functions
            if (nplans > 0) {
                appendStringInfoString(buf, ", plans:");
                array_desc(buf, plans, sizeof(xlhp_freeze_plan), nplans,
                          &plan_elem_desc, &frz_offsets);
            }

            if (nredirected > 0) {
                appendStringInfoString(buf, ", redirected:");
                array_desc(buf, redirected, sizeof(OffsetNumber) * 2,
                          nredirected, &redirect_elem_desc, NULL);
            }

            if (ndead > 0) {
                appendStringInfoString(buf, ", dead:");
                array_desc(buf, nowdead, sizeof(OffsetNumber), ndead,
                          &offset_elem_desc, NULL);
            }

            if (nunused > 0) {
                appendStringInfoString(buf, ", unused:");
                array_desc(buf, nowunused, sizeof(OffsetNumber), nunused,
                          &offset_elem_desc, NULL);
            }
        }
    }
    // Handle VISIBLE operations
    else if (info == XLOG_HEAP2_VISIBLE) {
        xl_heap_visible *xlrec = (xl_heap_visible *) rec;
        appendStringInfo(buf, "snapshotConflictHorizon: %u, flags: 0x%02X",
                        xlrec->snapshotConflictHorizon, xlrec->flags);
    }
    // Handle MULTI_INSERT operations
    else if (info == XLOG_HEAP2_MULTI_INSERT) {
        xl_heap_multi_insert *xlrec = (xl_heap_multi_insert *) rec;
        bool isinit = (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE) != 0;

        appendStringInfo(buf, "ntuples: %d, flags: 0x%02X",
                        xlrec->ntuples, xlrec->flags);

        // Show offsets if not initializing page
        if (XLogRecHasBlockData(record, 0) && !isinit) {
            appendStringInfoString(buf, ", offsets:");
            array_desc(buf, xlrec->offsets, sizeof(OffsetNumber),
                      xlrec->ntuples, &offset_elem_desc, NULL);
        }
    }
    // Handle LOCK_UPDATED operations
    else if (info == XLOG_HEAP2_LOCK_UPDATED) {
        xl_heap_lock_updated *xlrec = (xl_heap_lock_updated *) rec;

        appendStringInfo(buf, "xmax: %u, off: %u, ",
                        xlrec->xmax, xlrec->offnum);
        infobits_desc(buf, xlrec->infobits_set, "infobits");
        appendStringInfo(buf, ", flags: 0x%02X", xlrec->flags);
    }
    // Handle NEW_CID operations
    else if (info == XLOG_HEAP2_NEW_CID) {
        xl_heap_new_cid *xlrec = (xl_heap_new_cid *) rec;

        appendStringInfo(buf, "rel: %u/%u/%u, tid: %u/%u",
                        xlrec->target_locator.spcOid,
                        xlrec->target_locator.dbOid,
                        xlrec->target_locator.relNumber,
                        ItemPointerGetBlockNumber(&(xlrec->target_tid)),
                        ItemPointerGetOffsetNumber(&(xlrec->target_tid)));
        appendStringInfo(buf, ", cmin: %u, cmax: %u, combo: %u",
                        xlrec->cmin, xlrec->cmax, xlrec->combocid);
    }
}
```