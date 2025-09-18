# disassembledLeaf

## Location
src/backend/access/gin/gindatapage.c: 68 - 102

## Overview
A structure type used in PostgreSQL's GIN (Generalized Inverted Index) implementation to represent a disassembled leaf page during page modification operations, providing an in-memory representation that facilitates reorganization and recompression.

## Definition


## Detailed Description
The  structure is a key component of PostgreSQL's GIN index leaf page management system. It provides an in-memory representation of a leaf page that has been broken down into manageable segments for modification operations such as insertions, deletions, and page splits. This structure enables efficient reorganization of posting list data within leaf pages while maintaining the compressed format used by GIN indexes.

The structure supports both legacy (pre-9.4) and current page formats, and includes provisions for Write-Ahead Logging (WAL) data generation when page modifications need to be logged for crash recovery purposes.

## Parameters / Member Variables
- : A doubly-linked list head containing leafSegmentInfo structures that represent the individual segments of the disassembled page
- : Pointer to the last segment that should remain on the left page during a page split operation
- : Total size in bytes of all segments that will be placed on the left page after a split
- : Total size in bytes of all segments that will be placed on the right page after a split
- : Boolean flag indicating whether the original page was stored in the pre-9.4 format on disk
- : Buffer containing WAL (Write-Ahead Log) data representing the reconstructed leaf page
- : Length of the WAL data buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - dlist_node
  - GinPostingList

- Called from (representative examples):
  - dataBeginPlaceToPageLeaf
  - dataExecPlaceToPageLeaf
  - ginVacuumPostingTreeLeaf
  - computeLeafRecompressWALData
  - dataPlaceToPageLeafRecompress
  - dataPlaceToPageLeafSplit
  - disassembleLeaf
  - addItemsToLeaf
  - leafRepackItems

## Notes and Other Information
- This structure is primarily used during GIN index maintenance operations where leaf pages need to be modified or split
- The segment-based approach allows for efficient handling of compressed posting lists without requiring full decompression of unchanged segments
- The split-related fields (lastleft, lsize, rsize) are populated by the leafRepackItems function when determining how to distribute segments across pages during a split
- WAL data generation is conditional and only performed when needed for crash recovery logging
- The structure supports backward compatibility with pre-PostgreSQL 9.4 page formats through the oldformat flag