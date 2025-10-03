# gistGetItupFromPage

## Location
[src/backend/access/gist/gistbuildbuffers.c:311-335](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistbuildbuffers.c#L311-L335)

## Overview
Retrieves the last index tuple from a buffer page and removes it from the page, returning a copy of the tuple to the caller.

## Definition

```c
static void
gistGetItupFromPage(GISTNodeBufferPage *pageBuffer, IndexTuple *itup)
```
## Detailed Description
This function extracts the most recently added index tuple from a buffer page using a LIFO (Last In, First Out) approach. It locates the tuple at the end of the free space area, creates a copy of it using palloc, and then marks the space previously occupied by the tuple as free again. The function assumes the page is not empty and includes an assertion to verify this precondition.

## Parameters / Member Variables
- `*pageBuffer`: Pointer to the GISTNodeBufferPage from which to retrieve the tuple
- `*itup`: Pointer to IndexTuple pointer where the copied tuple will be stored
## Dependencies
- Functions called/Symbols referenced:
  - PAGE_IS_EMPTY (macro)
  - BUFFER_PAGE_DATA_OFFSET (macro)
  - PAGE_FREE_SPACE (macro)
  - IndexTupleSize
  - [palloc](../p/palloc.md)
  - memcpy
  - MAXALIGN (macro)
- Called from (representative examples):
  - [gistPopItupFromNodeBuffer](gistPopItupFromNodeBuffer.md)

## Notes and Other Information
- This is a static function, only accessible within the gistbuildbuffers.c file
- The function implements a stack-like behavior by always retrieving the last added tuple
- The caller is responsible for freeing the memory allocated for the copied tuple
- Uses an assertion to ensure the page is not empty before attempting retrieval
- Updates the page's free space counter to reflect the released space
- Essential for the buffer management during GiST index construction when tuples need to be moved between buffers