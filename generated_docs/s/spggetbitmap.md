# spggetbitmap

## Location
[src/backend/access/spgist/spgscan.c:942-958](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgscan.c#L942-L958)

## Overview
Main entry point for SP-GiST bitmap index scans that collects all matching heap tuple identifiers into a TID bitmap.

## Definition
int64 spggetbitmap(IndexScanDesc scan, TIDBitmap *tbm)

## Detailed Description
This function implements the bitmap scan interface for SP-GiST indexes. It initializes the scan state for bitmap collection, sets up the tuple bitmap manager, and delegates the actual tree traversal to spgWalk with the storeBitmap callback function. Unlike tuple-by-tuple scans, bitmap scans collect all matching tuple identifiers at once and return the total count. The function ensures that the scan processes the entire index by passing true for scanWholeIndex to spgWalk.

## Parameters / Member Variables
- : IndexScanDesc structure containing the scan descriptor with index relation and scan keys
- : TIDBitmap pointer to the tuple bitmap where matching tuple identifiers will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [spgWalk](spgWalk.md)
  - [storeBitmap](storeBitmap.md)
  - [IndexScanDesc](../I/IndexScanDesc.md)
  - [TIDBitmap](../T/TIDBitmap.md)
  - SpGistScanOpaque
- Called from (representative examples):
  - [spghandler](spghandler.md) (as part of the SP-GiST access method interface)

## Notes and Other Information
- This is a public function that implements part of the SP-GiST access method API
- Returns the total number of matching tuples found (ntids)
- Sets want_itup to false since bitmap scans don't need to reconstruct index tuples
- Always scans the whole index rather than stopping at page boundaries
- The collected bitmap can be used by higher-level code for efficient heap access
- Located at src/backend/access/spgist/spgscan.c:942-958