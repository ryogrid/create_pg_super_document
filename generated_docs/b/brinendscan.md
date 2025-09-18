# brinendscan

## Location
src/backend/access/brin/brin.c: 968 - 984

## Overview
Closes down a BRIN index scan by cleaning up allocated resources including the revmap access structure and BRIN descriptor.

## Definition


## Detailed Description
The brinendscan function is responsible for properly terminating a BRIN index scan and freeing all associated resources. This is part of the standard PostgreSQL index access method interface and is called when a scan is complete or being aborted.

The function performs three key cleanup operations:
1. Terminates the revmap (reverse mapping) access structure
2. Frees the BRIN descriptor that contains metadata about the index
3. Frees the opaque scan state structure

This ensures that all memory allocated during scan initialization (brinbeginscan) is properly released and any open resources are closed.

## Parameters / Member Variables
- : IndexScanDesc containing the scan state to be cleaned up, including the opaque BRIN-specific data

## Dependencies
- Functions called/Symbols referenced:
  - [brinRevmapTerminate](brinRevmapTerminate.md): Cleans up and closes the revmap access structure
  - [brin_free_desc](brin_free_desc.md): Frees the BRIN descriptor and associated metadata
  - [pfree](../p/pfree.md): Frees the BrinOpaque structure
- Called from (representative examples):
  - [brinhandler](brinhandler.md): BRIN access method handler registration

## Notes and Other Information
- This function is called automatically by the PostgreSQL executor when a scan completes
- Must be called to avoid memory leaks from BRIN scan operations
- The function assumes the scan->opaque field contains a valid BrinOpaque structure
- Part of the standard index access method interface that all PostgreSQL index types must implement
- No return value since cleanup operations are expected to always succeed