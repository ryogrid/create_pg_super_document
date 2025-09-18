# multirange_gist_compress

## Location
[src/backend/utils/adt/rangetypes_gist.c:245-269](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_gist.c#L245-L269)

## Overview
Implements the GiST compress operation for multirange types, converting multiranges into single ranges for efficient indexing by computing their union range.

## Definition


## Detailed Description
The  function is part of the GiST operator class for multirange types. It compresses multirange values into single range values for storage in GiST indexes. The compression works by computing the union range that spans all ranges within the multirange, effectively creating a bounding range that encompasses the entire multirange.

This compression is essential for GiST indexing because it allows multiranges to be represented and indexed as single ranges, enabling efficient spatial indexing operations. The function only performs compression for leaf entries (actual data values), while internal nodes are passed through unchanged.

## Parameters / Member Variables
- : GiST index entry containing the multirange key to be compressed

## Dependencies
- Functions called/Symbols referenced:
  - 
  - 
  - 
  - 
  - 
  - 
  - 
- Called from (representative examples):
  - GiST index access method during index construction and maintenance

## Notes and Other Information
- Only compresses leaf entries; internal nodes are returned unchanged
- The compression creates a bounding range that may include gaps not present in the original multirange
- This approach enables multiranges to leverage the existing range GiST infrastructure
- The compressed representation is used for index operations but the original multirange is preserved in the heap
- Located in src/backend/utils/adt/rangetypes_gist.c:245-269