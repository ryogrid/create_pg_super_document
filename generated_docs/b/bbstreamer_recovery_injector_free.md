# bbstreamer_recovery_injector_free

## Location
src/bin/pg_basebackup/bbstreamer_inject.c: 209 - 218

## Overview
Frees memory associated with the recovery injector bbstreamer, ensuring proper cleanup of the streaming pipeline.

## Definition


## Detailed Description
This function implements the memory deallocation operation for the bbstreamer_recovery_injector. It follows the standard bbstreamer cleanup pattern by first freeing the next bbstreamer in the chain, then freeing the current streamer instance. This ensures that the entire streaming pipeline is properly deallocated in reverse order, preventing memory leaks and maintaining proper resource management.

The function uses PostgreSQL's memory management functions to safely deallocate the bbstreamer structure that was allocated during initialization.

## Parameters / Member Variables
- : The bbstreamer instance to be freed

## Dependencies
- Functions called/Symbols referenced:
  - bbstreamer_free
  - pfree
  - bbstreamer (struct type)
- Called from (representative examples):
  - No direct references found (likely called via function pointer in operations table)

## Notes and Other Information
- Static function used as part of the bbstreamer_recovery_injector operations table
- Follows standard bbstreamer cleanup pattern: free next, then free self
- Ensures proper memory management and prevents leaks in the streaming pipeline
- Uses PostgreSQL's pfree function for memory deallocation
- Part of the standard bbstreamer lifecycle management
- Located in src/bin/pg_basebackup/bbstreamer_inject.c:209-218