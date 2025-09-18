# xl_brin_desummarize

## Location
src/include/access/brin_xlog.h: 133 - 140

## Overview
The  structure represents WAL (Write-Ahead Log) record data for BRIN index range de-summarization operations, storing the necessary information to replay the removal of a summarized range during recovery.

## Definition


## Detailed Description
This structure is used in PostgreSQL's BRIN (Block Range Index) access method for WAL logging when a range summary is removed (de-summarized). BRIN indexes maintain summaries for ranges of heap blocks, and when these summaries need to be invalidated or removed, this operation must be logged for crash recovery purposes.

The structure contains the minimal information needed to replay a de-summarization operation during WAL recovery: the range configuration, the specific heap block being de-summarized, and the location of the index tuple to be removed.

During de-summarization, two backup blocks are registered:
- Backup block 0: revmap page (containing the reverse mapping from heap blocks to index tuples)  
- Backup block 1: regular index page (containing the actual index tuple to be deleted)

## Parameters / Member Variables
- : The number of heap pages covered by each BRIN range summary - this configuration parameter is needed to locate the correct revmap entry
- : The heap block number whose range summary is being removed - used to identify which revmap entry should be invalidated
- : The offset number of the index tuple to delete from the regular index page - specifies the exact location of the tuple being removed

## Dependencies
- Functions called/Symbols referenced:
  - BlockNumber (type)
  - OffsetNumber (type)
- Called from (representative examples):
  - brinRevmapDesummarizeRange (src/backend/access/brin/brin_revmap.c:411)
  - brin_xlog_desummarize_page (src/backend/access/brin/brin_xlog.c:272, 276)
  - brin_desc (src/backend/access/rmgrdesc/brindesc.c:66)
  - SizeOfBrinDesummarize (src/include/access/brin_xlog.h:142)

## Notes and Other Information
- This structure is part of the BRIN WAL record format and is used specifically for the  WAL record type
- The size of this structure is computed by the  macro using  to ensure proper alignment
- De-summarization occurs when a BRIN range summary needs to be removed, typically during index maintenance operations
- The WAL record ensures that de-summarization operations can be properly replayed during crash recovery, maintaining index consistency
- This structure works in conjunction with the revmap (reverse mapping) system that tracks which heap block ranges have active summaries