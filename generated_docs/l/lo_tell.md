# lo_tell

## Location
[src/interfaces/libpq/fe-lobj.c:515-547](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-lobj.c#L515-L547)

## Overview
Returns the current seek location (file position) within a PostgreSQL large object.

## Definition

```c
int
lo_tell(PGconn *conn, int fd)
```
## Detailed Description
The  function retrieves the current position within an open large object, similar to the standard C library  function. It communicates with the PostgreSQL server using the internal large object function  to get the current seek position. The function returns the position as a 32-bit integer, which limits it to handling large objects up to 2GB in size. For larger objects,  should be used instead.

The function first ensures that the large object function OIDs are properly initialized through , then makes a server function call using  to retrieve the current position.

## Parameters / Member Variables
- : PostgreSQL database connection handle
- : File descriptor of the open large object

## Dependencies
- Functions called/Symbols referenced:
  - [lo_initialize](lo_initialize.md)
  - [PQfn](../P/PQfn.md)
  - [PQclear](../P/PQclear.md)
  - [PQresultStatus](../P/PQresultStatus.md)
- Types referenced:
  - PQArgBlock
  - [PGresult](../P/PGresult.md)
  - PGRES_COMMAND_OK
- Called from (representative examples):
  - Client applications using libpq large object interface

## Notes and Other Information
- Returns -1 on error or if the large object functions are not properly initialized
- Limited to 32-bit positions (2GB maximum large object size)
- For large objects exceeding 2GB, use  instead
- The function blocks until the server responds with the current position
- Part of PostgreSQL's libpq large object interface