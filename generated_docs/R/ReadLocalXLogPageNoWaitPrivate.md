# ReadLocalXLogPageNoWaitPrivate

## Location
src/include/access/xlogutils.h: 79 - 82

## Overview
A private data structure used as a callback context for non-blocking WAL (Write-Ahead Log) page reading operations to track when the end of available WAL data has been reached.

## Definition


## Detailed Description
ReadLocalXLogPageNoWaitPrivate is a simple private data structure that serves as callback context for the  function. This struct is designed to communicate state information between the WAL reading infrastructure and its callers when performing non-blocking WAL page reads.

The structure is used specifically in scenarios where the caller does not want to wait for future WAL data to become available. When  is called with , and the requested WAL location exceeds the currently available WAL data, the function sets the  flag to true in this structure to inform the caller that no more WAL data is currently available.

This mechanism is essential for applications like logical decoding that need to process WAL records but should not block when reaching the end of available WAL data.

## Parameters / Member Variables
- : Boolean flag indicating whether the end of currently available WAL data has been reached during a non-blocking read operation

## Dependencies
- Functions called/Symbols referenced:
  - (No direct function calls - this is a data structure)
- Used by:
  -  (src/backend/access/transam/xlogutils.c:946, 952)
  -  (indirectly through read_local_xlog_page_guts)

## Notes and Other Information
- This struct is typically allocated and managed by the caller of 
- The struct is passed through the  field
- It provides a clean interface for non-blocking WAL reading operations to communicate end-of-data conditions without using error codes
- The structure is minimal by design, containing only the essential state needed for non-blocking WAL page reading
- Used primarily in logical decoding contexts where blocking on WAL availability is not desired