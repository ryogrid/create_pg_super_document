# brinRevmapTerminate

## Location
[src/backend/access/brin/brin_revmap.c:100-111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_revmap.c#L100-L111)

## Overview
Releases all resources associated with a BRIN revmap access object and cleans up memory allocations.

## Definition
```c
void brinRevmapTerminate(BrinRevmap *revmap)
```

## Detailed Description
This function is the cleanup counterpart to brinRevmapInitialize, responsible for properly releasing all resources held by a BrinRevmap structure. It ensures that buffer pins are released and memory is freed to prevent resource leaks. The function handles both the metadata buffer that is always present and the optional current working buffer that may or may not be held depending on the revmap's state.

The cleanup process involves:
1. Releasing the metadata buffer pin that was acquired during initialization
2. Conditionally releasing the current working buffer if one is held
3. Freeing the memory allocated for the BrinRevmap structure itself

## Parameters / Member Variables
- `revmap`: The BrinRevmap access object to be terminated and freed

## Dependencies
- Functions called/Symbols referenced:
  - ReleaseBuffer
  - [pfree](../p/pfree.md)
- Types referenced:
  - [BrinRevmap](../B/BrinRevmap.md)
  - InvalidBuffer
- Called from:
  - [brininsertcleanup](brininsertcleanup.md)
  - [brinendscan](brinendscan.md)
  - [brinbuild](brinbuild.md)
  - [brinsummarize](brinsummarize.md) (multiple locations)
  - [brinRevmapDesummarizeRange](brinRevmapDesummarizeRange.md) (multiple locations)

## Notes and Other Information
- This function must be called for every BrinRevmap object created by brinRevmapInitialize to prevent resource leaks
- The function safely handles cases where rm_currBuf is InvalidBuffer (no current buffer held)
- Should be called even if errors occur during revmap operations to ensure proper cleanup
- The revmap pointer becomes invalid after this call and should not be used subsequently