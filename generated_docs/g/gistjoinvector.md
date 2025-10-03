# gistjoinvector

## Location
[src/backend/access/gist/gistutil.c:113-125](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistutil.c#L113-L125)

## Overview
Concatenates two IndexTuple arrays into a single enlarged array by expanding the first array and appending the second array's elements.

## Definition
```c
IndexTuple *gistjoinvector(IndexTuple *itvec, int *len, IndexTuple *additvec, int addlen)
```

## Detailed Description
This function efficiently combines two arrays of IndexTuple pointers into a single array. It reallocates the first array to accommodate the additional elements from the second array, then uses memmove to copy the contents of the second array to the end of the expanded first array. The function updates the length parameter to reflect the new total size and returns the enlarged array. The original first array is expanded in place (though its memory location may change due to reallocation), while the second array remains unchanged and can be freed by the caller if needed.

## Parameters / Member Variables
- `itvec`: The primary IndexTuple array to be expanded (modified in place)
- `len`: Pointer to the current length of itvec, updated to new total length
- `additvec`: The additional IndexTuple array to be appended
- `addlen`: Number of elements in the additvec array

## Dependencies
- Functions called/Symbols referenced:
  - [repalloc](../r/repalloc.md)
  - memmove
  - [IndexTupleData](../I/IndexTupleData.md)
- Called from (representative examples):
  - [gistplacetopage](gistplacetopage.md)
  - [gist_indexsortbuild_levelstate_flush](gist_indexsortbuild_levelstate_flush.md)

## Notes and Other Information
The function uses memmove instead of memcpy to safely handle potential memory overlap scenarios, though in typical usage patterns overlap should not occur. The function modifies the first array in place and returns the potentially relocated array pointer, so callers must use the returned value and not rely on the original itvec pointer remaining valid. This function is commonly used during page reorganization and bulk loading operations where multiple tuple vectors need to be combined efficiently.

## Simplified Source

```c
IndexTuple *gistjoinvector(IndexTuple *itvec, int *len, IndexTuple *additvec, int addlen) {
    // Expand first array to accommodate additional elements
    itvec = (IndexTuple *) repalloc(itvec, sizeof(IndexTuple) * ((*len) + addlen));

    // Copy additional elements to end of expanded array
    memmove(&itvec[*len], additvec, sizeof(IndexTuple) * addlen);

    // Update total length
    *len += addlen;

    return itvec;
}
```