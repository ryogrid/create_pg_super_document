# storeBitmap

## Location
src/backend/access/spgist/spgscan.c: 931 - 941

## Overview
A specialized result storage function used during SP-GiST bitmap scans to add matching heap tuple pointers to a tuple bitmap.

## Definition
static void storeBitmap(SpGistScanOpaque so, ItemPointer heapPtr, Datum leafValue, bool isnull, SpGistLeafTuple leafTuple, bool recheck, bool recheckDistances, double *distances)

## Detailed Description
This function serves as a storeRes callback specifically for bitmap index scans in SP-GiST. When spgWalk finds tuples that match the scan conditions, it calls this function to store the results. The function adds the heap tuple pointer to the tuple bitmap manager (TBM) and increments the count of found tuples. It asserts that distance-related parameters are not used since bitmap scans do not involve distance calculations or ordering.

## Parameters / Member Variables
- : SpGistScanOpaque structure containing scan state including the tuple bitmap manager
- : ItemPointer to the heap tuple that matched the scan conditions
- : Datum value from the leaf tuple (unused in bitmap scans)
- : Boolean indicating if the leaf value is null (unused in bitmap scans)
- : SpGistLeafTuple that matched (unused in bitmap scans)
- : Boolean indicating if the tuple needs to be rechecked by higher-level code
- : Boolean for distance rechecking (must be false for bitmap scans)
- : Array of distances (must be NULL for bitmap scans)

## Dependencies
- Functions called/Symbols referenced:
  - [tbm_add_tuples](../t/tbm_add_tuples.md)
  - SpGistScanOpaque
  - SpGistLeafTuple
- Called from (representative examples):
  - [spggetbitmap](spggetbitmap.md) (via spgWalk)

## Notes and Other Information
- This is a static function internal to spgscan.c
- Specifically designed for bitmap scans, not tuple-by-tuple retrieval
- Asserts that distance-related parameters are not used since bitmap scans don't support ordering
- Increments the ntids counter to track the number of matching tuples found
- Located at src/backend/access/spgist/spgscan.c:931-941