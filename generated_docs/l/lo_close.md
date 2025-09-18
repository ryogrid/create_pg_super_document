# lo_close

## Location
src/interfaces/libpq/fe-lobj.c: 96 - 130

## Overview
Closes an open large object file descriptor, releasing associated resources and completing any pending operations.

## Definition
```c
int lo_close(PGconn *conn, int fd)
```

## Detailed Description
The `lo_close` function closes a previously opened large object file descriptor, ensuring that any buffered data is flushed and resources are properly released. This function should be called for every file descriptor returned by `lo_open` to prevent resource leaks. The function communicates with the PostgreSQL backend through the function call interface (`PQfn`) to perform the close operation on the server side.

## Parameters / Member Variables
- `conn`: PostgreSQL connection handle for the database session
- `fd`: File descriptor of the large object to close, as returned by `lo_open`

## Dependencies
- Functions called/Symbols referenced:
  - [lo_initialize](lo_initialize.md)
  - PQfn
  - [PQresultStatus](../P/PQresultStatus.md)
  - [PQclear](../P/PQclear.md)
- Called from (representative examples):
  - [EndRestoreLO](../E/EndRestoreLO.md)
  - [dumpLOs](../d/dumpLOs.md)
  - [lo_import_internal](lo_import_internal.md)
  - [lo_export](lo_export.md)
  - [importFile](../i/importFile.md)
  - [exportFile](../e/exportFile.md)

## Notes and Other Information
- Returns 0 on success, -1 on failure
- Should be called for every file descriptor returned by lo_open to prevent resource leaks
- The file descriptor becomes invalid after a successful close operation
- Automatically handles communication with the PostgreSQL backend to complete the close operation
- Used extensively in cleanup paths and error handling throughout the large object API
- Essential for proper resource management in applications that work with large objects