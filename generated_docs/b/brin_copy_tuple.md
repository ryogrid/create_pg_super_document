# brin_copy_tuple

## Location
[src/backend/access/brin/brin_tuple.c:446-464](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_tuple.c#L446-L464)

## Overview
Creates a copy of a BRIN tuple with intelligent memory management, supporting buffer reuse to avoid frequent allocation/deallocation cycles.

## Definition

```c
BrinTuple *
brin_copy_tuple(BrinTuple *tuple, Size len, BrinTuple *dest, Size *destsz)
```
## Detailed Description
This function creates a copy of an existing BRIN tuple with optimized memory management designed for bulk operations. It supports an optional destination buffer that can be reused and resized as needed, which is particularly beneficial when processing many tuples in loops. The function intelligently handles three scenarios: allocating new memory when no destination is provided, reusing existing buffer when it's large enough, or expanding the buffer when more space is needed.

The function performs a simple memory copy operation after ensuring adequate buffer space, making it efficient for scenarios where tuple copying is performed frequently. The optional buffer reuse mechanism helps reduce memory allocation overhead in performance-critical code paths.

## Parameters / Member Variables
- : Source BrinTuple to be copied
- : Size of the source tuple in bytes
- : Optional destination buffer; if NULL, new memory is allocated
- : Pointer to size of destination buffer; updated if buffer is reallocated, can be NULL

## Dependencies
- Functions called/Symbols referenced:
  - [BrinTuple](../B/BrinTuple.md) (structure type)
  - [palloc](../p/palloc.md) (memory allocation)
  - [repalloc](../r/repalloc.md) (memory reallocation)
  - memcpy (memory copy operation)
- Called from:
  - [brininsert](brininsert.md) (src/backend/access/brin/brin.c:456)
  - [bringetbitmap](bringetbitmap.md) (src/backend/access/brin/brin.c:746)
  - [summarize_range](../s/summarize_range.md) (src/backend/access/brin/brin.c:1856)
  - [brin_evacuate_page](brin_evacuate_page.md) (src/backend/access/brin/brin_pageops.c:591)
  - BrinTupleIsEmptyRange (src/include/access/brin_tuple.h:101)

## Notes and Other Information
- Optimized for bulk processing scenarios where many tuples need to be copied
- Buffer reuse mechanism reduces memory allocation overhead in loops
- Supports both allocation of new memory and reuse of existing buffers
- Essential for BRIN index operations that require tuple duplication
- Used in various BRIN operations including insertion, bitmap scans, and page evacuation
- The destination buffer size tracking helps optimize memory usage patterns

## Simplified Source

```c
BrinTuple *brin_copy_tuple(BrinTuple *tuple, Size len, BrinTuple *dest, Size *destsz) {
    // Allocate new buffer if none provided or size is zero
    if (!destsz || *destsz == 0) {
        dest = palloc(len);
    }
    // Expand buffer if needed
    else if (len > *destsz) {
        dest = repalloc(dest, len);
        *destsz = len;
    }

    // Copy tuple data and return destination
    memcpy(dest, tuple, len);
    return dest;
}
```