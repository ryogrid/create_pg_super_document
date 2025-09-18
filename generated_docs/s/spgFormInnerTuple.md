# spgFormInnerTuple

## Location
src/backend/access/spgist/spgutils.c: 994 - 1076

## Overview
Constructs an SP-GiST inner tuple containing the given prefix and node array, used for building internal nodes in SP-GiST index structures.

## Definition


## Detailed Description
The  function creates an SP-GiST inner tuple that represents an internal node in the SP-GiST index tree. It combines a prefix value (if present) with an array of child node tuples into a single, properly formatted tuple structure. The function carefully calculates the required size, validates constraints, and assembles the components into a contiguous memory layout that can be stored on disk pages.

The function performs several important validations:
- Ensures the tuple size doesn't exceed page capacity limits
- Validates that header fields don't overflow their allocated bit ranges
- Guarantees the tuple is large enough to be replaced with a dead tuple later
- Uses maxaligned node tuple sizes for proper memory alignment

## Parameters / Member Variables
- : Pointer to SpGistState containing index configuration and type information
- : Boolean flag indicating whether the tuple should include a prefix value
- : The prefix datum to include (only used if hasPrefix is true)
- : Number of child node tuples to include in the inner tuple
- : Array of SpGistNodeTuple pointers representing the child nodes

## Dependencies
- Functions called/Symbols referenced:
  -  - calculates storage size for prefix data
  -  - gets size of individual node tuples
  -  - copies prefix data with proper type handling
  -  - allocates zeroed memory for the tuple
  -  - macro to get pointer to prefix data area
  -  - macro to get pointer to node array area
- Called from (representative examples):
  -  - when adding new nodes during insertion
  -  - during node splitting operations
  -  - when performing split actions

## Notes and Other Information
- The function ensures proper memory alignment by relying on node tuples being maxaligned
- Size calculations include header overhead (SGITHDRSZ) plus prefix and node data
- The minimum size constraint (SGDTSIZE) allows future replacement with dead tuples
- Error checking prevents index corruption by validating size limits and header field ranges
- The resulting tuple structure supports efficient page-based storage and retrieval operations