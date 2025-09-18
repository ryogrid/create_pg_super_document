# ginReadTuple

## Location
src/backend/access/gin/ginentrypage.c: 162 - 200

## Overview
Reads item pointers from a leaf entry tuple in PostgreSQL GIN index, handling both compressed and uncompressed posting lists.

## Definition
```c
ItemPointer ginReadTuple(GinState *ginstate, OffsetNumber attnum, IndexTuple itup, int *nitems)
```

## Detailed Description
The ginReadTuple function extracts item pointers from a GIN leaf entry tuple, supporting both compressed and uncompressed formats. For compressed tuples, it decodes the posting list using ginPostingListDecode and validates that the decoded count matches the header count. For uncompressed tuples, it directly copies the ItemPointer array. The function allocates memory for the result array and returns the count through the nitems parameter.

## Parameters / Member Variables
- `ginstate`: GIN state structure containing index metadata
- `attnum`: Attribute number (column number) for the key 
- `itup`: IndexTuple containing the posting list data
- `nitems`: Output parameter - receives the number of items in the posting list

## Dependencies
- Functions called/Symbols referenced:
  - GinGetPosting: Gets pointer to posting list data in the tuple
  - GinGetNPosting: Gets number of posting list items from tuple header
  - GinItupIsCompressed: Checks if tuple contains compressed posting list
  - [ginPostingListDecode](ginPostingListDecode.md): Decodes compressed posting list into ItemPointer array
  - [palloc](../p/palloc.md): Allocates memory for result array
  - memcpy: Copies uncompressed ItemPointer data

- Called from (representative examples):
  - [collectMatchBitmap](../c/collectMatchBitmap.md): Collecting matching items during GIN scan operations
  - [startScanEntry](../s/startScanEntry.md): Starting scan entry operations
  - [addItemPointersToLeafTuple](../a/addItemPointersToLeafTuple.md): Adding item pointers to leaf tuples during insertion

## Notes and Other Information
- Returns a pallocd array that must be freed by the caller