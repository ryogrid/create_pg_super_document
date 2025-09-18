# gistbuildempty

## Location
src/backend/access/gist/gist.c: 133 - 158

## Overview
Builds an empty GiST index in the initialization fork, creating and initializing the root page as a leaf page.

## Definition


## Detailed Description
This function creates an empty GiST index by initializing its root page in the initialization fork. The initialization fork is used in PostgreSQL for creating initial pages of indexes that will be populated later. The function extends the buffered relation to get a new buffer, initializes it as a leaf page (since an empty index has only a root page which is also a leaf), logs the operation for crash recovery, and then releases the buffer.

The function performs the operation within a critical section to ensure atomicity and proper crash recovery. The newly created page is marked as dirty and logged using the write-ahead logging (WAL) system to ensure durability.

## Parameters / Member Variables
- : The Relation structure representing the GiST index to be initialized

## Dependencies
- Functions called/Symbols referenced:
  - ExtendBufferedRel (extends the relation with a new buffer)
  - BMR_REL (buffer manager relation macro)
  - GISTInitBuffer (initializes the buffer as a GiST page)
  - MarkBufferDirty (marks buffer as needing to be written)
  - log_newpage_buffer (logs the new page for WAL)
  - UnlockReleaseBuffer (unlocks and releases the buffer)
  - START_CRIT_SECTION/END_CRIT_SECTION (critical section macros)
- Constants used:
  - INIT_FORKNUM (initialization fork number)
  - EB_SKIP_EXTENSION_LOCK (skip extension lock flag)
  - EB_LOCK_FIRST (lock first flag)
  - F_LEAF (leaf page flag)
- Called from:
  - gisthandler (assigned as ambuildempty callback at src/backend/access/gist/gist.c:87)

## Notes and Other Information
- This function is part of the access method interface for GiST indexes
- The root page is created as a leaf page since an empty index contains no internal nodes
- Uses the initialization fork rather than the main fork for crash safety during index creation
- The critical section ensures that the page initialization and logging are atomic operations
- Located in src/backend/access/gist/gist.c:133-158