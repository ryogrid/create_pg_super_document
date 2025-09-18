# tuplestore_select_read_pointer

## Location
src/backend/utils/sort/tuplestore.c: 473 - 545

## Overview
Switches the active read pointer in a tuplestore to the specified pointer, handling file positioning and state management for different storage modes.

## Definition


## Detailed Description
This function makes the specified read pointer active in the tuplestore. When multiple read pointers exist, this function allows switching between them while maintaining the correct file positions. The function handles three different tuplestore states:

1. **TSS_INMEM/TSS_WRITEFILE**: No special handling needed as data is in memory
2. **TSS_READFILE**: Saves the current file position of the old active pointer and seeks to the position of the new active pointer

For file-based storage, the function carefully manages file positioning by:
- Saving the current file position before switching
- Seeking to the new position (either EOF for eof_reached pointers or the stored file position)
- Handling errors during file operations

## Parameters / Member Variables
- : Pointer to the  structure containing the tuplestore
- : Index of the read pointer to make active (must be >= 0 and < readptrcount)

## Dependencies
- Functions called/Symbols referenced:
  -  - gets current file position
  -  - seeks to specific file position  
  -  - reports errors
  -  - logs error messages
  - Constants: , , 
- Called from (representative examples):
  -  (nodeCtescan.c:47, 115)
  -  (nodeCtescan.c:246)
  -  (nodeNamedtuplestorescan.c:42)
  -  (nodeWindowAgg.c:1529, 1610, 1686)
  -  (nodeWindowAgg.c:3227, 3235)

## Notes and Other Information
- Returns immediately if the requested pointer is already active
- Uses Assert to validate pointer index bounds
- Critical for window functions that need multiple cursors into the same tuplestore
- Handles both EOF and non-EOF pointer positions correctly
- File seek operations can fail and will raise PostgreSQL errors
- Used extensively in CTE (Common Table Expression) scanning and window aggregate operations