# GISTNodeBufferPage

## Location
src/include/access/gist_private.h: 51 - 52

## Overview
GISTNodeBufferPage represents the structure of a buffer page used during GiST index construction to temporarily store tuples before they are written to disk pages.

## Definition


## Detailed Description
GISTNodeBufferPage is a data structure used internally during GiST index building to manage temporary storage of index tuples. It serves as a buffer page that can hold multiple tuples before they are flushed to actual disk pages. The structure is designed to efficiently manage space and maintain links between buffer pages through the prev field.

The structure uses a flexible array member for tupledata, allowing it to store variable amounts of tuple data within a single allocated block. The freespace field tracks available space, enabling efficient space management and preventing overflow.

## Parameters / Member Variables
- : BlockNumber pointing to the previous buffer page, forming a linked list of buffer pages
- : Amount of free space remaining in this buffer page for storing additional tuples
- : Flexible array member containing the actual index tuple data stored in this buffer page

## Dependencies
- Functions called/Symbols referenced:
  - BlockNumber (for prev field)
  - FLEXIBLE_ARRAY_MEMBER (for tupledata array)
- Called from (representative examples):
  - gistAllocateNewPageBuffer (allocates and initializes new buffer pages)
  - gistGetNodeBuffer (retrieves buffer pages)
  - gistPlaceItupToPage (places tuples into buffer pages)
  - gistGetItupFromPage (retrieves tuples from buffer pages)
  - gistPushItupToNodeBuffer (adds tuples to node buffers)

## Notes and Other Information
- Buffer pages are allocated with BLCKSZ size to match PostgreSQL's standard block size
- The BUFFER_PAGE_DATA_OFFSET macro calculates the offset where tuple data begins
- Related macros PAGE_FREE_SPACE, PAGE_IS_EMPTY, and PAGE_NO_SPACE provide convenient operations on buffer pages
- Used primarily during GiST index construction phase, not during normal index operations
- Memory is allocated in the build context and automatically freed when construction completes