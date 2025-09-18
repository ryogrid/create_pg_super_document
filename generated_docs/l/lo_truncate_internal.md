# lo_truncate_internal

## Location
src/backend/libpq/be-fsstubs.c: 553 - 573

## Overview
Internal static function that truncates an open large object to a specified length using a file descriptor.

## Definition
static void lo_truncate_internal(int32 fd, int64 len)

## Detailed Description
This internal function provides the core logic for truncating PostgreSQL large objects. It operates on large objects that have already been opened and are identified by a file descriptor. The function validates that the descriptor is valid and that the large object was opened with write permissions before performing the truncation operation.

The function performs several validation steps:
1. Validates that the file descriptor is within valid range and points to an active large object
2. Checks that the large object was opened with write permissions (IFS_WRLOCK flag)
3. Delegates the actual truncation operation to the lower-level inv_truncate function

This is a static helper function shared by both be_lo_truncate (32-bit length) and be_lo_truncate64 (64-bit length) functions.

## Parameters / Member Variables
- : Large object file descriptor (32-bit integer identifying the open large object)
- : Target length for truncation (64-bit integer specifying the new size in bytes)

## Dependencies
- Functions called/Symbols referenced:
  - inv_truncate
  - LargeObjectDesc (structure type)
- Called from (representative examples):
  - be_lo_truncate
  - be_lo_truncate64

## Notes and Other Information
- This is a static function, only accessible within be-fsstubs.c
- Uses the global cookies array to track open large object descriptors
- Requires the large object to be opened with write permissions (IFS_WRLOCK flag)
- Performs comprehensive validation of file descriptor validity
- The actual truncation logic is handled by the inv_truncate function
- Supports 64-bit length values for large objects exceeding 2GB
- Error handling includes specific error codes for different failure modes