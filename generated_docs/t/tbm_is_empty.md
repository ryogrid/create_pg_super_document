# tbm_is_empty

## Location
[src/backend/nodes/tidbitmap.c:670-688](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/tidbitmap.c#L670-L688)

## Overview
Checks whether a TIDBitmap is completely empty, returning true if no tuple identifiers are stored in the bitmap.

## Definition
```c
bool tbm_is_empty(const TIDBitmap *tbm)
```

## Detailed Description
This function provides a simple and efficient way to determine if a TIDBitmap contains any tuple identifiers. It performs this check by examining the nentries field of the TIDBitmap structure, which maintains a count of the total number of entries (both individual pages and chunks) currently stored in the bitmap. The function is designed to be lightweight and fast, making it suitable for use in query optimization decisions and execution flow control.

## Parameters / Member Variables
- `tbm`: A constant pointer to the TIDBitmap structure to be checked for emptiness

## Dependencies
- Functions called/Symbols referenced:
  - [TIDBitmap](../T/TIDBitmap.md) (structure type)
- Called from (representative examples):
  - [startScanEntry](../s/startScanEntry.md) (src/backend/access/gin/ginget.c:386)
  - [MultiExecBitmapAnd](../M/MultiExecBitmapAnd.md) (src/backend/executor/nodeBitmapAnd.c:155)

## Notes and Other Information
- The function is implemented as a simple comparison of tbm->nentries == 0
- This is a read-only operation that does not modify the bitmap state
- The function is commonly used in bitmap scan operations to determine if there are any tuples to iterate over
- It serves as an optimization point where empty bitmaps can be handled efficiently without setting up iteration machinery

## Simplified Source

```c
bool tbm_is_empty(const TIDBitmap *tbm)
{
    return (tbm->nentries == 0);
}
```