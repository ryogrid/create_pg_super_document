# dataExecPlaceToPageLeaf

## Location
[src/backend/access/gin/gindatapage.c:716-737](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/gindatapage.c#L716-L737)

## Overview
dataExecPlaceToPageLeaf performs the actual data insertion into a GIN data leaf page after space allocation has been confirmed, executing within a critical section with WAL logging support.

## Definition

```c
static void
dataExecPlaceToPageLeaf(GinBtree btree, Buffer buf, GinBtreeStack *stack,
						void *insertdata, void *ptp_workspace)
```
## Detailed Description
This function is the execution phase of GIN data page insertion, called after  has determined that the insertion will fit on the target page. It operates within a critical section and handles the actual modification of the page content along with WAL logging if required.

The function takes a pre-prepared disassembledLeaf structure from the workspace and applies the changes to the target buffer. It then marks the buffer as dirty to ensure the changes are persisted. For WAL-enabled relations, it registers the buffer and associated WAL data that was previously computed during the planning phase.

This is part of PostgreSQL's GIN index data page management system, specifically handling leaf page modifications in a transactionally safe manner.

## Parameters / Member Variables
- : GIN B-tree context containing index relation and build state information
- : Target buffer containing the leaf page to be modified
- : GIN B-tree stack representing the path to the current page (unused in this function)
- : Data to be inserted (unused directly, changes are applied via workspace)
- : Pre-prepared disassembledLeaf structure containing the modified page content and WAL information

## Dependencies
- Functions called/Symbols referenced:
  - [dataPlaceToPageLeafRecompress](dataPlaceToPageLeafRecompress.md)
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
  - RelationNeedsWAL
  - [XLogRegisterBuffer](../X/XLogRegisterBuffer.md)
  - [XLogRegisterBufData](../X/XLogRegisterBufData.md)
  - [disassembledLeaf](disassembledLeaf.md) (structure type)
  - [GinBtree](../G/GinBtree.md) (structure type)
  - [GinBtreeStack](../G/GinBtreeStack.md) (structure type)
  - REGBUF_STANDARD (constant)
- Called from (representative examples):
  - [dataExecPlaceToPage](dataExecPlaceToPage.md)

## Notes and Other Information
- This function must be called within a critical section
- WAL record creation should already be started before calling this function
- The target buffer must be registered in slot 0 for WAL logging
- The function assumes that space availability has already been verified
- WAL logging is conditional based on relation requirements and build state
- The actual page recompression is delegated to dataPlaceToPageLeafRecompress

## Simplified Source

```c
static void
dataExecPlaceToPageLeaf(GinBtree btree, Buffer buf, GinBtreeStack *stack,
                        void *insertdata, void *ptp_workspace)
{
    disassembledLeaf *leaf = (disassembledLeaf *) ptp_workspace;

    // Apply the leaf page changes using recompression
    dataPlaceToPageLeafRecompress(buf, leaf);

    // Mark buffer as modified
    MarkBufferDirty(buf);

    // Log changes for crash recovery if needed
    if (RelationNeedsWAL(btree->index) && !btree->isBuild)
    {
        XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
        XLogRegisterBufData(0, leaf->walinfo, leaf->walinfolen);
    }
}
```