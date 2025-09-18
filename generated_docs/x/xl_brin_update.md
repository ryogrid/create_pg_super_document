# xl_brin_update

## Location
src/include/access/brin_xlog.h: 87 - 93

## Overview
A WAL record structure for logging BRIN tuple update operations, specifically cross-page updates where the new tuple is placed on a different page than the old tuple.

## Definition


## Detailed Description
The  structure extends the basic  structure to handle BRIN tuple updates that span across pages (cross-page updates). This occurs when a BRIN tuple needs to be updated but there isn't sufficient space on the original page to accommodate the new tuple, requiring it to be placed on a different page.

This WAL record manages three backup blocks: backup block 0 contains the new page with the new BrinTuple data, backup block 1 contains the revmap page (for mapping heap blocks to index tuples), and backup block 2 contains the old page where the original tuple resided. The structure inherits all the insertion-related information from  while adding the necessary information to handle the removal of the old tuple.

## Parameters / Member Variables
- : The offset number of the old tuple on the old page that is being replaced
- : An embedded  structure containing all the information needed for inserting the new tuple (heapBlk, pagesPerRange, and offnum for the new tuple location)

## Dependencies
- Functions called/Symbols referenced:
  - OffsetNumber (type)
  - [xl_brin_insert](xl_brin_insert.md) (embedded structure)
- Called from (representative examples):
  - [brin_doupdate](../b/brin_doupdate.md) (in src/backend/access/brin/brin_pageops.c:271)
  - [brin_xlog_update](../b/brin_xlog_update.md) (in src/backend/access/brin/brin_xlog.c:138)
  - [brin_desc](../b/brin_desc.md) (in src/backend/access/rmgrdesc/brindesc.c:44)
  - SizeOfBrinUpdate (macro in src/include/access/brin_xlog.h:95)

## Notes and Other Information
- This structure is specifically for cross-page updates where the old and new tuples are on different pages
- Uses three backup blocks compared to the two blocks used by 
- The structure composition allows code reuse by embedding the insert structure rather than duplicating its fields
- For same-page updates, a different structure () is used instead
- The  macro calculates the total size including the embedded  structure
- This design pattern demonstrates PostgreSQL's approach to extending WAL record structures while maintaining backward compatibility