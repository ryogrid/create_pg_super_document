# gistXLogPageReuse

## Location
[src/backend/access/gist/gistxlog.c:594-628](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistxlog.c#L594-L628)

## Overview
Writes a WAL record when a previously deleted GiST page is being reused, primarily to establish conflict points for Hot Standby servers.

## Definition


## Detailed Description
The  function creates a WAL record when a GiST index page that was previously deleted is being reused for new data. Unlike most WAL records, this record doesn't describe a page modification but serves as a conflict point for Hot Standby servers to ensure read consistency.

When a page is deleted and later reused, transactions on standby servers that might still need to read the old version of the page need to be informed that the page is no longer available. This WAL record provides the necessary information for Hot Standby conflict resolution by recording the transaction ID that originally deleted the page.

The function records metadata about the relation, the specific block being reused, and the deletion transaction ID (snapshotConflictHorizon) that originally removed the page's content. It also tracks whether the associated heap relation is accessible in logical decoding, which affects how the reuse is handled.

## Parameters / Member Variables
- : The GiST index relation where the page reuse is occurring
- : The heap relation associated with the GiST index
- : Block number of the page being reused
- : Full transaction ID of the transaction that originally deleted the page content

## Dependencies
- Functions called/Symbols referenced:
  - RelationIsAccessibleInLogicalDecoding
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogInsert](../X/XLogInsert.md)
  - SizeOfGistxlogPageReuse
  - XLOG_GIST_PAGE_REUSE
  - RM_GIST_ID
- Called from (representative examples):
  - [gistNewBuffer](gistNewBuffer.md) (when obtaining a buffer for new page allocation)
  - Referenced in GISTPageSplitInfo structure

## Notes and Other Information
- This function does NOT register the buffer with the WAL record since no page modification occurs
- The primary purpose is Hot Standby conflict resolution rather than crash recovery
- The snapshotConflictHorizon field allows standby servers to determine which transactions need to be cancelled due to the page reuse
- The isCatalogRel flag affects logical decoding behavior for the reuse operation
- This is part of PostgreSQL's mechanism to maintain read consistency across primary and standby servers
- The record helps ensure that old snapshots on standby servers don't try to read pages that have been reused for different data