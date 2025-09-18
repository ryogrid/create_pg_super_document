# gistXLogPageDelete

## Location
src/backend/access/gist/gistxlog.c: 552 - 575

## Overview
Writes a WAL record describing a GiST page deletion operation, including the removal of the downlink from the parent page.

## Definition


## Detailed Description
The  function creates a WAL record for a GiST page deletion operation. This function is called during VACUUM operations when a GiST page becomes empty and needs to be deleted from the index structure. The WAL record captures both the deletion of the page itself and the removal of the corresponding downlink from its parent page.

The function creates a  record containing the transaction ID that performed the deletion and the offset of the downlink in the parent page that needs to be removed. It registers both the page being deleted and its parent page as standard buffer references in the WAL record, allowing the deletion to be properly replayed during recovery.

This is a critical operation for maintaining index consistency during crash recovery, ensuring that both the page deletion and parent page update are atomic operations that can be correctly reconstructed on standby servers.

## Parameters / Member Variables
- : Buffer containing the page to be deleted
- : Full transaction ID of the transaction performing the deletion (used for visibility checks during recovery)
- : Buffer containing the parent page that has a downlink to the page being deleted
- : Offset number within the parent page where the downlink to be removed is located

## Dependencies
- Functions called/Symbols referenced:
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogRegisterBuffer](../X/XLogRegisterBuffer.md)
  - [XLogInsert](../X/XLogInsert.md)
  - SizeOfGistxlogPageDelete
  - REGBUF_STANDARD
  - XLOG_GIST_PAGE_DELETE
  - RM_GIST_ID
- Called from (representative examples):
  - [gistdeletepage](gistdeletepage.md) (during VACUUM operations)
  - Referenced in GISTPageSplitInfo structure

## Notes and Other Information
- This function is primarily used during VACUUM operations when cleaning up empty pages from a GiST index
- The transaction ID is important for recovery scenarios to ensure proper visibility semantics
- Both the deleted page and its parent page are registered with REGBUF_STANDARD, meaning full page images may be included if needed for consistency
- The downlink offset allows the recovery process to locate and remove the exact index tuple pointing to the deleted page
- The function is part of PostgreSQL's crash recovery mechanism and must maintain ACID properties for the deletion operation