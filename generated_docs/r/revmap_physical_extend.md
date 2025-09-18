# revmap_physical_extend

## Location
src/backend/access/brin/brin_revmap.c: 522 - 645

## Overview
Attempts to extend the BRIN reverse mapping (revmap) by one physical page, handling complex concurrency scenarios and page management for BRIN indexes.

## Definition


## Detailed Description
This function is responsible for extending the BRIN reverse mapping structure by adding one new physical page. The BRIN reverse mapping is crucial for efficiently mapping heap block numbers to their corresponding index tuple locations. The function implements a careful protocol to handle concurrent operations and ensures data integrity during the extension process.

The function follows a multi-step process:
1. Locks the metapage exclusively to prevent concurrent extensions
2. Validates that cached metadata is up-to-date
3. Determines the next block number for the new revmap page
4. Either reads an existing block or extends the relation if needed
5. Handles page evacuation if the target block is already in use
6. Initializes the new page as a revmap page and updates metadata
7. Logs the operation for write-ahead logging (WAL) if required

The function is designed to be retry-safe, meaning callers are expected to retry the operation until the desired outcome is achieved, as various concurrency scenarios may prevent immediate success.

## Parameters / Member Variables
- : Pointer to the BrinRevmap structure containing the reverse mapping metadata and cached information

## Dependencies
- Functions called/Symbols referenced:
  - LockBuffer
  - BufferGetPage
  - PageGetContents
  - RelationGetNumberOfBlocks
  - ReadBuffer
  - ExtendBufferedRel
  - BufferGetBlockNumber
  - UnlockReleaseBuffer
  - PageIsNew
  - BRIN_IS_REGULAR_PAGE
  - BrinPageType
  - brin_start_evacuating_page
  - brin_evacuate_page
  - brin_page_init
  - MarkBufferDirty
  - RelationNeedsWAL
  - XLogBeginInsert
  - XLogRegisterData
  - XLogRegisterBuffer
  - XLogInsert
  - PageSetLSN
- Called from (representative examples):
  - revmap_extend_and_get_blkno

## Notes and Other Information
- This is a static function used internally within the BRIN revmap implementation
- The function handles several edge cases including concurrent relation extensions and page evacuation
- Uses critical sections (START_CRIT_SECTION/END_CRIT_SECTION) to ensure atomicity of metadata updates
- Implements proper WAL logging for crash recovery when RelationNeedsWAL returns true
- The function may return early without extending if concurrency conflicts are detected, requiring the caller to retry
- Page evacuation is performed when a target block is already in use as a regular BRIN page
- Maintains proper buffer locking protocols to prevent corruption during concurrent access
- Updates the metapage's pd_lower field to ensure proper page compression handling by xlog.c