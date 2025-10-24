# lo_open

## Location
[src/interfaces/libpq/fe-lobj.c:57-95](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-lobj.c#L57-L95)

## Overview
Opens an existing PostgreSQL large object and returns a file descriptor for subsequent large object operations.

## Definition

```c
int
lo_open(PGconn *conn, Oid lobjId, int mode)
```
## Detailed Description
The  function provides access to an existing large object in PostgreSQL by opening it and returning a file descriptor that can be used for subsequent read, write, seek, and other large object operations. The function initializes the large object function lookup table if necessary, then calls the PostgreSQL backend function  through the function call interface (). The operation mode parameter determines what operations are permitted on the opened large object.

## Parameters / Member Variables
- `*conn`: PostgreSQL connection handle for the database session
- `lobjId`: Object identifier (OID) of the large object to open
- `mode`: Access mode flags determining permitted operations (read, write, etc.)
## Dependencies
- Functions called/Symbols referenced:
  - [lo_initialize](lo_initialize.md)
  - [PQfn](../P/PQfn.md)
  - [PQresultStatus](../P/PQresultStatus.md)
  - [PQclear](../P/PQclear.md)
- Called from (representative examples):
  - [StartRestoreLO](../S/StartRestoreLO.md)
  - [dumpLOs](../d/dumpLOs.md)
  - [lo_import_internal](lo_import_internal.md)
  - [lo_export](lo_export.md)
  - [importFile](../i/importFile.md)
  - [exportFile](../e/exportFile.md)

## Notes and Other Information
- Returns -1 on failure, otherwise returns a valid file descriptor
- The returned file descriptor is used for subsequent large object operations
- Requires that the large object already exists in the database
- The function automatically initializes the large object function lookup table on first use
- Access mode determines what operations are permitted on the opened large object
- Used extensively in pg_dump utilities and test programs for large object manipulation

## Simplified Source

```c
int lo_open(PGconn *conn, Oid lobjId, int mode) {
    int fd;
    int result_len;
    PQArgBlock argv[2];
    PGresult *res;

    // Initialize large object function lookup table
    if (lo_initialize(conn) < 0)
        return -1;

    // Prepare arguments: large object ID and access mode
    argv[0].isint = 1;
    argv[0].len = 4;
    argv[0].u.integer = lobjId;

    argv[1].isint = 1;
    argv[1].len = 4;
    argv[1].u.integer = mode;

    // Call backend lo_open function
    res = PQfn(conn, conn->lobjfuncs->fn_lo_open, &fd, &result_len, 1, argv, 2);

    // Check result and return file descriptor or error
    if (PQresultStatus(res) == PGRES_COMMAND_OK) {
        PQclear(res);
        return fd; // Success: return file descriptor
    } else {
        PQclear(res);
        return -1; // Failure
    }
}
```