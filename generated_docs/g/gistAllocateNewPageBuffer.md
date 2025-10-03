# gistAllocateNewPageBuffer

## Location
[src/backend/access/gist/gistbuildbuffers.c:181-197](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistbuildbuffers.c#L181-L197)

## Overview
gistAllocateNewPageBuffer allocates and initializes a new buffer page for storing index tuples during GiST index construction.

## Definition

```c
static GISTNodeBufferPage *
gistAllocateNewPageBuffer(GISTBuildBuffers *gfbb)
```
## Detailed Description
This static function creates a new buffer page that serves as temporary storage for index tuples during the GiST index building process. The function allocates memory for a full block-sized page (BLCKSZ bytes) and initializes it with default values.

The allocated page is zero-initialized using MemoryContextAllocZero to ensure all fields start with clean values. The function sets up the page's metadata including the previous page pointer (set to InvalidBlockNumber for a new page) and calculates the available free space for storing tuples.

The page buffer is designed to hold multiple index tuples until it becomes full, at which point it may be written to the temporary file system managed by the GiST build buffers.

## Parameters / Member Variables
- `*gfbb`: The GiST build buffers structure that provides the memory context for allocation
## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md)
  - PAGE_FREE_SPACE (macro)
  - BUFFER_PAGE_DATA_OFFSET
  - BLCKSZ
  - InvalidBlockNumber
- Called from (representative examples):
  - [gistLoadNodeBuffer](gistLoadNodeBuffer.md)
  - [gistPushItupToNodeBuffer](gistPushItupToNodeBuffer.md)

## Notes and Other Information
- Function is declared static, making it internal to the gistbuildbuffers.c module
- Allocates exactly BLCKSZ bytes (typically 8KB) to match PostgreSQL's standard block size
- Uses MemoryContextAllocZero for zero-initialization, which is more efficient than separate allocation and memset
- The prev field is set to InvalidBlockNumber to indicate this is a new, unlinked page
- Free space calculation accounts for page header overhead via BUFFER_PAGE_DATA_OFFSET
- Memory is allocated in the build context to ensure proper lifetime management during index construction

## Simplified Source

```c
static GISTNodeBufferPage *
gistAllocateNewPageBuffer(GISTBuildBuffers *gfbb)
{
    GISTNodeBufferPage *pageBuffer;

    // Allocate and zero-initialize full block
    pageBuffer = (GISTNodeBufferPage *) MemoryContextAllocZero(gfbb->context, BLCKSZ);

    // Initialize page metadata
    pageBuffer->prev = InvalidBlockNumber;
    PAGE_FREE_SPACE(pageBuffer) = BLCKSZ - BUFFER_PAGE_DATA_OFFSET;

    return pageBuffer;
}
```