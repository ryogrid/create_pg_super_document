# lo_unlink

## Location
[src/interfaces/libpq/fe-lobj.c:589-625](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-lobj.c#L589-L625)

## Overview
Deletes a PostgreSQL large object from the database, permanently removing it and freeing its storage space.

## Definition

```c
int
lo_unlink(PGconn *conn, Oid lobjId)
```
## Detailed Description
The  function permanently deletes a large object from the PostgreSQL database. It operates similar to the Unix  system call, removing the large object identified by its OID from the database. Once deleted, the large object cannot be recovered and all its associated data is lost.

The function communicates with the PostgreSQL server using the internal large object function  to perform the deletion operation. It takes the large object's OID as a parameter rather than a file descriptor, since the object doesn't need to be open to be deleted.

## Parameters / Member Variables
- `*conn`: PostgreSQL database connection handle
- `lobjId`: Object identifier (OID) of the large object to delete
## Dependencies
- Functions called/Symbols referenced:
  - [lo_initialize](lo_initialize.md)
  - [PQfn](../P/PQfn.md)
  - [PQclear](../P/PQclear.md)
  - [PQresultStatus](../P/PQresultStatus.md)
- Types referenced:
  - Oid
  - PQArgBlock
  - [PGresult](../P/PGresult.md)
  - PGRES_COMMAND_OK
- Called from (representative examples):
  - [do_lo_unlink](../d/do_lo_unlink.md) (in psql's large_obj.c)
  - Client applications managing large object lifecycle

## Notes and Other Information
- Returns 1 on successful deletion, -1 on error
- The operation is permanent and cannot be undone
- Does not require the large object to be open before deletion
- Will fail if the large object doesn't exist or if the user lacks appropriate permissions
- Part of PostgreSQL's large object management interface
- Should be used with caution as deleted large objects cannot be recovered without backup restoration

## Simplified Source

```c
int lo_unlink(PGconn *conn, Oid lobjId) {
    PQArgBlock argv[1];
    PGresult *res;
    int result_len;
    int retval;

    // Initialize large object subsystem
    if (lo_initialize(conn) < 0)
        return -1;

    // Prepare large object OID argument
    argv[0].isint = 1;
    argv[0].len = 4;
    argv[0].u.integer = lobjId;

    // Call backend lo_unlink function via fastpath interface
    res = PQfn(conn, conn->lobjfuncs->fn_lo_unlink,
               &retval, &result_len, 1, argv, 1);

    // Return success (1) or failure (-1)
    if (PQresultStatus(res) == PGRES_COMMAND_OK) {
        PQclear(res);
        return retval;
    } else {
        PQclear(res);
        return -1;
    }
}
```