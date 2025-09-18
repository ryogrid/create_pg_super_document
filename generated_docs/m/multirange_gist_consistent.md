# multirange_gist_consistent

## Location
[src/backend/utils/adt/rangetypes_gist.c:270-323](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_gist.c#L270-L323)

## Overview
Implements the GiST consistency check operation for multirange types, determining whether a query matches an index entry during multirange index searches.

## Definition


## Detailed Description
The  function is the core consistency checking function for GiST indexes on multirange types. It determines whether a given query (which can be a range, multirange, or element) matches an index entry based on the specified search strategy. Since multiranges are stored in the index as compressed union ranges (via ), this function performs consistency checking against those compressed representations.

The function handles three types of queries:
1. Multirange queries (subtype is invalid or ANYMULTIRANGEOID)
2. Range queries (subtype is ANYRANGEOID)
3. Element queries (any other subtype)

It also distinguishes between leaf nodes (actual compressed data) and internal nodes (bounding ranges) in the GiST tree structure.

## Parameters / Member Variables
- : GiST index entry containing the key (compressed range) to check against
- : The search query value (range, multirange, or element)
- : Strategy number indicating the type of search operation (overlaps, contains, etc.)
- : OID indicating the query type (range, multirange, or element)
- : Output parameter set to true since operations are inexact due to compression

## Dependencies
- Functions called/Symbols referenced:
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
- Called from (representative examples):
  - GiST index access method during multirange query execution

## Notes and Other Information
- This function sets recheck to true because multirange operations are inexact due to the compression of multiranges into union ranges
- The compressed representation may include gaps not present in the original multirange, requiring recheck at the heap level
- The function acts as a dispatcher, routing to appropriate specialized consistency functions based on node type and query type
- Part of the GiST operator class implementation for multirange types
- Located in src/backend/utils/adt/rangetypes_gist.c:270-323