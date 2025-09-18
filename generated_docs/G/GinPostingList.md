# GinPostingList

## Location
src/include/access/ginblock.h: 341 - 342

## Overview
GinPostingList is a compressed data structure in PostgreSQL's GIN index implementation that stores a list of item pointers in a space-efficient format using variable-byte encoding.

## Definition


## Detailed Description
GinPostingList is a core data structure used in GIN indexes to store compressed lists of item pointers (TIDs) that point to heap tuples. The structure employs a hybrid approach where the first item pointer is stored uncompressed for quick access, while subsequent item pointers are stored using variable-byte encoding to minimize space usage. This compression is crucial for GIN indexes as posting lists can become very large, especially for common values. The variable-byte encoding takes advantage of the fact that item pointers in a posting list are typically stored in sorted order, allowing for efficient delta encoding. The structure requires 2-byte alignment and includes helper macros for size calculation and navigation between segments.

## Parameters / Member Variables
- : ItemPointerData containing the first item pointer in the posting list stored in uncompressed format for efficient access
- : uint16 value indicating the number of bytes that follow in the compressed bytes array
- : Flexible array member containing the variable-byte encoded representation of subsequent item pointers in the list

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerData](../I/ItemPointerData.md) (for the first uncompressed item pointer)
  - FLEXIBLE_ARRAY_MEMBER (for the variable-length bytes array)
- Called from (representative examples):
  - [GinDataLeafPageGetItems](GinDataLeafPageGetItems.md) (src/backend/access/gin/gindatapage.c:141)
  - [ginCompressPostingList](../g/ginCompressPostingList.md) (src/backend/access/gin/ginpostinglist.c:203)
  - [ginPostingListDecode](../g/ginPostingListDecode.md) (src/backend/access/gin/ginpostinglist.c:284)
  - [ginVacuumEntryPage](../g/ginVacuumEntryPage.md) (src/backend/access/gin/ginvacuum.c:490)
  - [disassembleLeaf](../d/disassembleLeaf.md) (src/backend/access/gin/gindatapage.c:1373)

## Notes and Other Information
- Requires 2-byte alignment as specified in the header comments
- Uses variable-byte encoding to compress item pointers, significantly reducing storage space
- The first item is stored uncompressed to serve as a base for delta encoding of subsequent items
- Helper macros SizeOfGinPostingList and GinNextPostingListSegment are provided for memory management
- Critical for GIN index performance as it allows storing large posting lists efficiently
- Used in both leaf pages of posting trees and as compressed representations in entry tuples
- The compression algorithm is optimized for sorted item pointer sequences common in GIN indexes
- Supports segmentation for very large posting lists that cannot fit in a single page