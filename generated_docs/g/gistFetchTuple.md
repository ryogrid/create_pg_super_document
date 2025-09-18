# gistFetchTuple

## Location
[src/backend/access/gist/gistutil.c:666-722](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistutil.c#L666-L722)

## Overview
Reconstructs the originally-indexed data from a GiST index tuple by fetching/decompressing all key attributes and copying included attributes, returning a new HeapTuple suitable for index-only scans.

## Definition
```c
HeapTuple gistFetchTuple(GISTSTATE *giststate, Relation r, IndexTuple tuple)
```

## Detailed Description
This function processes a GiST index tuple to reconstruct the original tuple data that was indexed. For each key attribute, it determines whether fetching is possible based on the operator class capabilities: if a fetch function exists, it calls gistFetchAtt to decompress the value; if neither fetch nor compress functions exist, it uses the stored value directly; otherwise, it marks the attribute as NULL since index-only scans aren't supported. For included attributes, it copies them directly from the index tuple. The function operates in a temporary memory context and returns a HeapTuple formed from the reconstructed data.

## Parameters / Member Variables
- `giststate`: GiST state information containing operator class functions, tuple descriptors, and temporary memory context
- `r`: The GiST index relation
- `tuple`: The IndexTuple from which to extract and reconstruct the original data

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - IndexRelationGetNumberOfKeyAttributes
  - [index_getattr](../i/index_getattr.md)
  - [gistFetchAtt](gistFetchAtt.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - INDEX_MAX_KEYS (constant)
- Called from (representative examples):
  - [gistScanPage](gistScanPage.md)

## Notes and Other Information
- Used primarily for index-only scans where the original tuple data must be reconstructed from the index
- The function handles three scenarios for key attributes: fetch function available, no compression/fetch functions (direct storage), and unsupported index-only scan (NULL substitution)
- Included attributes are always stored in their original form and copied directly
- Uses temporary memory context to avoid memory leaks during tuple reconstruction
- The resulting HeapTuple is formed using fetchTupdesc which describes the structure of the original indexed data
- Critical for enabling index-only scans on GiST indexes when the operator classes support fetching