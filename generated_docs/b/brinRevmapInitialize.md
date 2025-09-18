# brinRevmapInitialize

## Location
src/backend/access/brin/brin_revmap.c: 70 - 99

## Overview
Initializes an access object for a BRIN (Block Range Index) range map, which provides the mapping from heap block ranges to index tuples.

## Definition


## Detailed Description
This function creates and initializes a BrinRevmap structure that serves as the access interface to a BRIN index's range map. The range map is a critical component of BRIN indexes that maintains the mapping between heap block ranges and their corresponding index tuples. The function reads the index's metadata page to extract essential parameters like pages per range and the last revmap page number, then constructs the access object with this information.

The function performs the following key operations:
1. Reads and locks the BRIN metadata page to access index configuration
2. Extracts metadata including pages per range and last revmap page
3. Allocates and initializes a BrinRevmap structure
4. Sets up the revmap with index relation, range parameters, and buffer management
5. Returns the configured revmap object for subsequent operations

## Parameters / Member Variables
- : The BRIN index relation for which to initialize the revmap
- : Output parameter that receives the number of heap pages covered by each index range

## Dependencies
- Functions called/Symbols referenced:
  - ReadBuffer
  - LockBuffer  
  - BufferGetPage
  - PageGetContents
  - palloc
- Types referenced:
  - BrinRevmap
  - BrinMetaPageData
  - BRIN_METAPAGE_BLKNO
  - BUFFER_LOCK_SHARE
  - BUFFER_LOCK_UNLOCK
- Called from:
  - initialize_brin_insertstate
  - brinbeginscan
  - brinbuild
  - brinsummarize
  - brinRevmapDesummarizeRange

## Notes and Other Information
- The returned BrinRevmap object must be freed using brinRevmapTerminate when no longer needed
- The function holds a shared lock on the metadata page during initialization but releases it before returning
- The metadata buffer (rm_metaBuf) remains held in the revmap structure for later use
- This is typically the first function called when beginning any BRIN revmap operations