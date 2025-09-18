# out_gistxlogPageDelete

## Location
src/backend/access/rmgrdesc/gistdesc.c: 52 - 60

## Overview
A static function that formats and outputs information about GiST page deletion WAL records, including the deletion transaction ID and downlink offset details.

## Definition
```c
static void out_gistxlogPageDelete(StringInfo buf, gistxlogPageDelete *xlrec)
```

## Detailed Description
This function handles the formatting and output of GiST index page deletion operations stored in WAL records. Page deletions occur when pages become empty and can be safely removed from the index structure. This operation requires careful tracking of transaction IDs to ensure proper visibility and recovery behavior.

The function outputs critical information including the transaction ID of the last transaction that could see the page during a scan, and the offset of the downlink in the parent page that referenced the deleted page.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the formatted output will be written
- `xlrec`: Pointer to a gistxlogPageDelete structure containing:
  - `deleteXid`: FullTransactionId of the last transaction that could see this page in a scan (critical for visibility rules)
  - `downlinkOffset`: OffsetNumber indicating the position of the downlink in the parent page that referenced the deleted page

## Dependencies
- Functions called/Symbols referenced:
  - gistxlogPageDelete (struct type)
  - EpochFromFullTransactionId (extracts epoch from FullTransactionId)
  - XidFromFullTransactionId (extracts transaction ID from FullTransactionId)
  - appendStringInfo (StringInfo formatting function)
- Called from (representative examples):
  - gist_desc (when processing XLOG_GIST_PAGE_DELETE records)

## Notes and Other Information
- Output format: "deleteXid epoch:xid; downlink offset"
- The deleteXid is crucial for determining when the deleted page can be safely reused
- Page deletion involves two backup blocks: the deleted page and the parent page containing the downlink
- The downlinkOffset helps identify which entry in the parent page was removed to complete the deletion
- This operation is part of GiST index maintenance to reclaim empty pages
- Located in src/backend/access/rmgrdesc/gistdesc.c at lines 52-60