# GinScanEntry

## Location
src/include/access/gin_private.h: 266 - 267

## Overview
GinScanEntry is a pointer typedef that references GinScanEntryData structures, representing individual search conditions extracted from GIN index scan queries.

## Definition
```c
typedef struct GinScanEntryData *GinScanEntry;
```

## Detailed Description
GinScanEntry serves as a pointer type for accessing GinScanEntryData structures within the PostgreSQL GIN access method. Each GinScanEntry represents a specific index search condition that has been extracted from one or more qualifier expressions. Multiple GinScanKey objects can point to the same GinScanEntry when identical search conditions are found across different qualifier expressions, which provides important efficiency benefits especially for full-index-scan operations.

GinScanEntry objects are used throughout the GIN scan process to track the state of individual search conditions, including their current position in posting trees, match results, and other scan-related metadata stored in the underlying GinScanEntryData structure.

## Parameters / Member Variables
This is a typedef pointer, so it has no direct members. It points to a GinScanEntryData structure.

## Dependencies
- Functions called/Symbols referenced:
  - GinScanEntryData (the structure it points to)
- Called from (representative examples):
  - scanPostingTree (src/backend/access/gin/ginget.c:69)
  - collectMatchBitmap (src/backend/access/gin/ginget.c:122)  
  - startScanEntry (src/backend/access/gin/ginget.c:319)
  - startScanKey (src/backend/access/gin/ginget.c:545)
  - entryLoadMoreItems (src/backend/access/gin/ginget.c:655)
  - entryGetItem (src/backend/access/gin/ginget.c:810)
  - keyGetItem (src/backend/access/gin/ginget.c:999)
  - matchPartialInPendingList (src/backend/access/gin/ginget.c:1543)
  - ginbeginscan (src/backend/access/gin/ginscan.c:56)
  - ginFillScanEntry (src/backend/access/gin/ginscan.c:63)
  - ginNewScanKey (src/backend/access/gin/ginscan.c:293)

## Notes and Other Information
- Defined in src/include/access/gin_private.h:266-267
- Part of the GIN index internal API used for scan operations
- Allows for efficient sharing of identical search conditions across multiple scan keys
- Used extensively in GIN posting tree traversal and tuple matching operations
- The underlying GinScanEntryData contains detailed state information for the search condition including current heap item pointers, match bitmaps, and posting list data