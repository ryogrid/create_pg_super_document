# GinScanKey

## Location
src/include/access/gin_private.h: 264 - 265

## Overview
GinScanKey is a pointer typedef that references GinScanKeyData structures, representing individual GIN index qualifier expressions during index scans.

## Definition


## Detailed Description
GinScanKey serves as a convenient pointer type for accessing GinScanKeyData structures in the PostgreSQL GIN (Generalized Inverted Index) access method. Each GinScanKey represents a single qualifier expression from a query that will be evaluated against the GIN index. The actual functionality and data are contained within the GinScanKeyData structure that this pointer references.

From the source comments, GinScanKey describes a single GIN index qualifier expression, from which one or more specific index search conditions are extracted and represented by GinScanEntryData. Multiple GinScanKeyData.scanEntry pointers can reference the same GinScanEntryData when identical search conditions are requested by different qualifier expressions, which is important for efficiency especially when dealing with full-index-scan entries.

## Parameters / Member Variables
This is a typedef pointer, so it has no direct members. It points to a GinScanKeyData structure.

## Dependencies
- Functions called/Symbols referenced:
  - [GinScanKeyData](GinScanKeyData.md) (the structure it points to)
- Called from (representative examples):
  - [entryIndexByFrequencyCmp](../e/entryIndexByFrequencyCmp.md) (src/backend/access/gin/ginget.c:490)
  - [startScanKey](../s/startScanKey.md) (src/backend/access/gin/ginget.c:505)
  - [keyGetItem](../k/keyGetItem.md) (src/backend/access/gin/ginget.c:992)
  - [scanGetItem](../s/scanGetItem.md) (src/backend/access/gin/ginget.c:1323)
  - [collectMatchesForHeapRow](../c/collectMatchesForHeapRow.md) (src/backend/access/gin/ginget.c:1623)
  - [ginInitConsistentFunction](../g/ginInitConsistentFunction.md) (src/backend/access/gin/ginlogic.c:227)
  - [ginScanKeyAddHiddenEntry](../g/ginScanKeyAddHiddenEntry.md) (src/backend/access/gin/ginscan.c:142)
  - [ginFillScanKey](../g/ginFillScanKey.md) (src/backend/access/gin/ginscan.c:164)
  - [ginNewScanKey](../g/ginNewScanKey.md) (src/backend/access/gin/ginscan.c:286)

## Notes and Other Information
- Defined in src/include/access/gin_private.h:264-265
- This typedef is part of the GIN index internal API and is used extensively throughout the GIN scan implementation
- The pointer allows for efficient passing of scan key information between various GIN access method functions
- Multiple scan keys can reference the same scan entries when identical search conditions are found, providing memory efficiency and performance optimization