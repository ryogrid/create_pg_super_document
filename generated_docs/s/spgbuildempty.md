# spgbuildempty

## Location
[src/backend/access/spgist/spginsert.c:154-182](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spginsert.c#L154-L182)

## Overview
Creates an empty SP-GiST index structure in the initialization fork for use during database initialization or recovery.

## Definition


## Detailed Description
This function creates the minimal SP-GiST index structure required for an empty index using the bulk write API. It constructs the three essential pages: metapage, root page, and null-tuples page, writing them directly to the initialization fork. Unlike spgbuild(), this function does not scan any heap data and creates only the skeletal index structure. This is typically used during database initialization, template database creation, or when creating empty indexes that will be populated later.

## Parameters / Member Variables
- : The SP-GiST index relation for which to create the empty structure

## Dependencies
- Functions called/Symbols referenced:
  - [smgr_bulk_start_rel](smgr_bulk_start_rel.md)
  - [smgr_bulk_get_buf](smgr_bulk_get_buf.md)
  - [SpGistInitMetapage](../S/SpGistInitMetapage.md)
  - [SpGistInitPage](../S/SpGistInitPage.md)
  - [smgr_bulk_write](smgr_bulk_write.md)
  - [smgr_bulk_finish](smgr_bulk_finish.md)
  - [BulkWriteState](../B/BulkWriteState.md)
  - BulkWriteBuffer
- Called from (representative examples):
  - [spghandler](spghandler.md)

## Notes and Other Information
Uses the bulk write API for efficient page creation and writes to the INIT_FORKNUM. The function ensures all three required SP-GiST pages are created with proper initialization and written to their designated block numbers (SPGIST_METAPAGE_BLKNO, SPGIST_ROOT_BLKNO, SPGIST_NULL_BLKNO).