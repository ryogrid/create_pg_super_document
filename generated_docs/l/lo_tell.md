# lo_tell

## Location
src/interfaces/libpq/fe-lobj.c: 515 - 547

## Overview
Returns the current seek location (file position) within a PostgreSQL large object.

## Definition


## Detailed Description
The  function retrieves the current position within an open large object, similar to the standard C library  function. It communicates with the PostgreSQL server using the internal large object function  to get the current seek position. The function returns the position as a 32-bit integer, which limits it to handling large objects up to 2GB in size. For larger objects,  should be used instead.

The function first ensures that the large object function OIDs are properly initialized through , then makes a server function call using  to retrieve the current position.

## Parameters / Member Variables
- : PostgreSQL database connection handle
- : File descriptor of the open large object

## Dependencies
- Functions called/Symbols referenced:
  - [lo_initialize](lo_initialize.md)
  - PQfn
  - [PQclear](../P/PQclear.md)
  - [PQresultStatus](../P/PQresultStatus.md)
- Types referenced:
  - PQArgBlock
  - PGresult
  - PGRES_COMMAND_OK
- Called from (representative examples):
  - Client applications using libpq large object interface

## Notes and Other Information
- Returns -1 on error or if the large object functions are not properly initialized
- Limited to 32-bit positions (2GB maximum large object size)
- For large objects exceeding 2GB, use  instead
- The function blocks until the server responds with the current position
- Part of PostgreSQL's libpq large object interface