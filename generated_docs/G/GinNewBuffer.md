# GinNewBuffer

## Location
[src/backend/access/gin/ginutil.c:300-337](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginutil.c#L300-L337)

## Overview
Allocates a new buffer page for a GIN index by either recycling a free page or extending the index file, returning a pinned and exclusive-locked buffer.

## Definition

```c
Buffer
GinNewBuffer(Relation index)
```
## Detailed Description
The  function is responsible for obtaining a new page for use in a GIN index. It follows a two-phase allocation strategy:

1. **Free Space Map (FSM) Phase**: First attempts to recycle existing free pages by querying the Free Space Map. It loops through available free pages, attempting to lock each one conditionally to avoid blocking. For each page, it verifies that the page is actually recyclable using  before returning it.

2. **File Extension Phase**: If no recyclable pages are found in the FSM, the function extends the index file by adding a new page at the end using .

The returned buffer is guaranteed to be both pinned (preventing it from being evicted from the buffer pool) and exclusively locked, making it ready for immediate use. The caller is responsible for properly initializing the page content using  or similar initialization functions.

## Parameters / Member Variables
- : The Relation representing the GIN index for which to allocate a new buffer

## Dependencies
- Functions called/Symbols referenced:
  -  (query Free Space Map for available pages)
  -  (read a page into buffer pool)
  -  (attempt non-blocking exclusive lock)
  -  (verify page can be reused)
  -  with  (release buffer lock)
  -  (release buffer from buffer pool)
  -  with flags , ,  (extend index file)
  -  (get page from buffer)
  -  (constant for invalid block)

- Called from:
  -  (src/backend/access/gin/ginbtree.c:465, 495)
  -  (src/backend/access/gin/gindatapage.c:1824)
  -  (src/backend/access/gin/ginfast.c:164)
  -  (src/backend/access/gin/gininsert.c:340, 343)

## Notes and Other Information
- The function implements a robust page allocation strategy that prioritizes reusing existing free space before expanding the file
- Uses conditional locking to avoid deadlocks when multiple processes are simultaneously trying to recycle the same page
- The loop continues until a genuinely recyclable page is found or all FSM suggestions are exhausted
- Pages from the FSM may not actually be recyclable due to concurrent activity, hence the need for verification
- The returned buffer requires initialization by the caller using appropriate GIN page initialization functions
- File extension is performed with  to ensure the new page is immediately locked
- This function is critical for GIN index growth and maintenance operations
- The function handles the complexity of concurrent access to free pages in a multi-user environment
- Caller must ensure proper cleanup (unlocking and unpinning) of the returned buffer when done