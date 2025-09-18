# range_gist_consistent

## Location
src/backend/utils/adt/rangetypes_gist.c: 191 - 244

## Overview
Implements the GiST consistency check operation for range types, determining whether a query matches an index entry during index searches.

## Definition


## Detailed Description
The  function is the core consistency checking function for GiST indexes on range types. It determines whether a given query (which can be a range, multirange, or element) matches an index entry based on the specified search strategy. This function serves as the entry point that dispatches to specialized consistency checking functions based on whether the index entry is a leaf or internal node, and what type of query is being performed.

The function handles three types of queries:
1. Range queries (subtype is invalid or ANYRANGEOID)
2. Multirange queries (subtype is ANYMULTIRANGEOID)  
3. Element queries (any other subtype)

It also distinguishes between leaf nodes (actual data) and internal nodes (bounding ranges) in the GiST tree structure.

## Parameters / Member Variables
- : GiST index entry containing the key (range) to check against
- : The search query value (range, multirange, or element)
- : Strategy number indicating the type of search operation (overlaps, contains, etc.)
- : OID indicating the query type (range, multirange, or element)
- : Output parameter set to false since all operations are exact

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
  - GiST index access method during query execution

## Notes and Other Information
- This function sets recheck to false because all range operations are exact and don't require rechecking at the heap level
- The function acts as a dispatcher, routing to appropriate specialized consistency functions based on node type and query type
- Part of the GiST operator class implementation for range types
- Located in src/backend/utils/adt/rangetypes_gist.c:191-244