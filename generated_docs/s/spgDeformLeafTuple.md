# spgDeformLeafTuple

## Location
[src/backend/access/spgist/spgutils.c:1107-1151](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgutils.c#L1107-L1151)

## Overview
Converts an SP-GiST leaf tuple into separate Datum/isnull arrays, extracting the individual column values from the tuple's packed storage format.

## Definition


## Detailed Description
The  function decomposes an SP-GiST leaf tuple back into individual column values, converting from the compact on-disk storage format to separate arrays of Datums and null indicators. This function handles the tuple's null bitmap and works correctly for both regular data trees and special null-value trees.

The function includes special handling for the trivial case where there's only a key attribute in a nulls tree, though this is currently dead code. For normal cases, it delegates to  after setting up the appropriate pointers to the tuple data and null bitmap sections.

## Parameters / Member Variables
- : The SP-GiST leaf tuple to be deformed
- : Tuple descriptor defining the structure and types of the tuple columns
- : Output array to store the extracted column values (caller must allocate sufficient space)
- : Output array to store null indicators for each column (caller must allocate sufficient space)
- : Flag indicating whether the key column is null (for consistency checking)

## Dependencies
- Functions called/Symbols referenced:
  -  - macro to check if tuple has a null bitmap
  -  - macro to calculate leaf tuple header size
  -  - core tuple deformation function
  -  - constant identifying the key column index
  -  - structure defining leaf tuple layout
- Called from (representative examples):
  -  - during node splitting operations to extract values
  -  - when retrieving tuples during index scans

## Notes and Other Information
- The caller must allocate sufficient storage for output arrays (INDEX_MAX_KEYS entries recommended)
- Contains assertions to ensure consistency between the keyColumnIsNull parameter and actual tuple content
- Handles both tuples with and without null bitmaps based on the hasNullsMask flag
- The special case for single-attribute nulls trees is currently unreachable but maintained for completeness
- Critical for index scan operations where tuple values need to be extracted and processed
- Works in conjunction with the tuple formation functions to provide round-trip conversion between storage and working formats