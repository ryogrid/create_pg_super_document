# computeLeafRecompressWALData

## Location
src/backend/access/gin/gindatapage.c: 872 - 977

## Overview
computeLeafRecompressWALData constructs WAL record data for GIN leaf page recompression operations, encoding segment modifications into a format suitable for crash recovery replay.

## Definition


## Detailed Description
This function prepares Write-Ahead Logging (WAL) data for GIN data leaf page recompression operations by analyzing a disassembledLeaf structure and encoding the changes into a ginxlogRecompressDataLeaf record format. The function must be called before entering the critical section that performs the actual page updates because it requires memory allocation.

The function first counts all modified segments, then allocates a buffer large enough to hold the WAL record header plus all segment data. It iterates through each segment, recording the segment number and action type, followed by the appropriate data based on the action. For efficiency, it can optimize ADDITEMS actions by converting them to REPLACE actions when the compressed segment data is smaller than the uncompressed item pointer list.

The resulting WAL data contains a complete description of all changes needed to reconstruct the leaf page modifications during recovery, including deletions, insertions, replacements, and item additions. The function stores the constructed WAL buffer and its length in the disassembledLeaf structure for later use during WAL record creation.

## Parameters / Member Variables
- : Pointer to disassembledLeaf structure containing segment modification information; function updates leaf->walinfo and leaf->walinfolen fields

## Dependencies
- Functions called/Symbols referenced:
  - dlist_foreach
  - dlist_container
  - palloc
  - memcpy
  - elog
  - SizeOfGinPostingList
  - SHORTALIGN
  - leafSegmentInfo (structure type)
  - ginxlogRecompressDataLeaf (structure type)
  - dlist_iter (structure type)
  - GIN_SEGMENT_UNMODIFIED (constant)
  - GIN_SEGMENT_DELETE (constant)
  - GIN_SEGMENT_ADDITEMS (constant)
  - GIN_SEGMENT_INSERT (constant)
  - GIN_SEGMENT_REPLACE (constant)
  - ItemPointerData (structure type)
- Called from (representative examples):
  - dataBeginPlaceToPageLeaf
  - ginVacuumPostingTreeLeaf

## Notes and Other Information
- Must be called before entering critical sections due to palloc usage
- Automatically optimizes ADDITEMS to REPLACE when compressed data is smaller
- Allocates maximum possible buffer size (BLCKSZ + overhead) to avoid complex size calculations
- WAL data format includes action count, followed by (segment_number, action, data) tuples
- Different actions store different data formats: DELETE stores nothing, ADDITEMS stores item count and items, INSERT/REPLACE store compressed segment data
- Uses SHORTALIGN for segment data alignment in WAL records
- Segment numbering excludes inserted segments until after processing
- The constructed WAL data enables complete page reconstruction during recovery replay