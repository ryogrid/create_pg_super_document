# lo_import

## Location
[src/interfaces/libpq/fe-lobj.c:626-640](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-lobj.c#L626-L640)

## Overview
Imports a file from the client filesystem into a PostgreSQL large object, automatically assigning a new OID.

## Definition

```c
Oid
lo_import(PGconn *conn, const char *filename)
```
## Detailed Description
The  function is a convenience wrapper that imports a file from the client's filesystem into a new PostgreSQL large object. It automatically creates a new large object with a system-assigned OID and copies the entire contents of the specified file into it. The function is implemented as a simple wrapper around  with  as the OID parameter, which tells the internal function to create a new large object with an automatically assigned OID.

The internal implementation opens the specified file in binary read mode, creates a new large object with read/write permissions, and then copies the file contents in chunks using the large object API. The function handles errors gracefully, cleaning up resources and providing appropriate error messages if any step fails.

## Parameters / Member Variables
- `*conn`: PostgreSQL database connection handle
- `*filename`: Path to the file on the client filesystem to import
## Dependencies
- Functions called/Symbols referenced:
  - [lo_import_internal](lo_import_internal.md)
  - InvalidOid (constant)
- Internal implementation uses:
  - [lo_creat](lo_creat.md)
  - [lo_open](lo_open.md)
  - [lo_write](lo_write.md)
  - [lo_close](lo_close.md)
- Called from (representative examples):
  - [do_lo_import](../d/do_lo_import.md) (in psql's large_obj.c)
  - [main](../m/main.md) (in testlo.c and testlo64.c test programs)

## Notes and Other Information
- Returns the OID of the newly created large object on success,  on failure
- The file is read from the client filesystem, not the server filesystem
- Creates large objects with both read and write permissions (INV_READ | INV_WRITE)
- Handles binary files correctly using O_RDONLY | PG_BINARY file opening mode
- For importing with a specific OID, use  instead
- The operation is transactional - if it fails partway through, the transaction is aborted
- File reading is done in chunks using LO_BUFSIZE buffer size for memory efficiency

## Simplified Source

```c
Oid lo_import(PGconn *conn, const char *filename)
{
    // Simple wrapper - delegate to internal function with auto-assigned OID
    return lo_import_internal(conn, filename, InvalidOid);
}
```