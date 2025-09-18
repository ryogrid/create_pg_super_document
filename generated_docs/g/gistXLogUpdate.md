# gistXLogUpdate

## Location
src/backend/access/gist/gistxlog.c: 629 - 669

## Overview
gistXLogUpdate creates a Write-Ahead Log (WAL) record for GiST index page updates that can include any number of tuple deletions and insertions on a single index page.

## Definition


## Detailed Description
gistXLogUpdate is responsible for writing WAL records that describe page updates in GiST (Generalized Search Tree) indexes. The function handles complex page modifications that can involve both deletions and insertions of tuples in a single operation. It constructs a WAL record containing all necessary information to replay the page update during recovery.

The function uses PostgreSQL's WAL infrastructure to ensure crash recovery and replication consistency. It registers the target buffer and associated data with the WAL system, allowing for both full page images and delta records depending on the WAL insertion logic.

Special handling is provided for page splits: if the update inserts a downlink for a split page, the function also records that the F_FOLLOW_RIGHT flag on the child page is cleared and the NSN (Next Sequence Number) is set.

## Parameters / Member Variables
- : The target buffer containing the GiST index page to be updated
- : Array of offset numbers identifying tuples to be deleted from the page
- : Number of tuples to delete (length of todelete array)
- : Array of IndexTuple pointers containing new tuples to insert
- : Number of tuples to insert (length of itup array)
- : Optional buffer for the left child page (used during page splits)

## Dependencies
- Functions called/Symbols referenced:
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogRegisterBuffer](../X/XLogRegisterBuffer.md)
  - [XLogRegisterBufData](../X/XLogRegisterBufData.md)
  - [XLogInsert](../X/XLogInsert.md)
  - IndexTupleSize
  - REGBUF_STANDARD
  - RM_GIST_ID
  - XLOG_GIST_PAGE_UPDATE
- Called from (representative examples):
  - [gistplacetopage](gistplacetopage.md)
  - [gistvacuumpage](gistvacuumpage.md)

## Notes and Other Information
- Both the todelete array and the tuples are marked as belonging to the target buffer, allowing XLogInsert to optimize by logging the whole buffer contents if deemed more efficient
- The function includes a full page image of the child buffer only when necessary (typically after a checkpoint following a page split)
- Returns an XLogRecPtr representing the LSN (Log Sequence Number) of the inserted WAL record
- The WAL record type used is XLOG_GIST_PAGE_UPDATE with resource manager RM_GIST_ID