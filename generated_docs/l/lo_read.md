# lo_read

## Location
src/interfaces/libpq/fe-lobj.c: 245 - 294

## Overview
Reads data from an open large object into a buffer, performing validation checks on the file descriptor and access permissions.

## Definition
```c
int lo_read(int fd, char *buf, int len)
```

## Detailed Description
The `lo_read` function is a backend implementation that reads data from a large object identified by its file descriptor. It validates that the file descriptor is valid and that the large object was opened with read permissions before delegating the actual read operation to the lower-level `inv_read` function. This function is part of the internal large object implementation and provides essential error checking to ensure safe access to large object data. The function operates at the byte level and assumes the large object supports byte-oriented reads and seeks.

## Parameters / Member Variables
- `fd`: File descriptor of the open large object (must be valid and opened for reading)
- `buf`: Buffer to store the read data
- `len`: Number of bytes to read from the large object

## Dependencies
- Functions called/Symbols referenced:
  - inv_read
  - ereport (for error reporting)
  - errcode
  - errmsg
- Called from (representative examples):
  - be_loread
  - dumpLOs
  - lo_export
  - pickout
  - exportFile

## Notes and Other Information
- Returns the number of bytes actually read, which may be less than requested if end of object is reached
- Validates file descriptor range and ensures the descriptor points to a valid large object
- Checks that the large object was opened with read permissions (IFS_RDLOCK flag)
- Throws ERROR with specific error codes for invalid descriptors or insufficient permissions
- Part of the backend large object implementation, not directly callable via function manager
- Assumes large objects support byte-oriented operations for simplified implementation
- Used by higher-level functions that provide the fmgr-callable interface
- Essential for data retrieval operations in PostgreSQL's large object subsystem