# lo_tell64

## Location
[src/interfaces/libpq/fe-lobj.c:548-588](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-lobj.c#L548-L588)

## Overview
Returns the current seek location within a PostgreSQL large object using 64-bit precision, supporting large objects larger than 2GB.

## Definition


## Detailed Description
The  function retrieves the current position within an open large object using 64-bit arithmetic, extending beyond the 2GB limitation of the 32-bit  function. This function is essential for handling very large objects in PostgreSQL. It communicates with the PostgreSQL server using the internal large object function  to get the current seek position as a 64-bit integer.

The function includes additional error checking to ensure that the  server function is available, as this function was added in later PostgreSQL versions. It also performs network byte order conversion using  to ensure proper handling of the 64-bit return value across different architectures.

## Parameters / Member Variables
- : PostgreSQL database connection handle
- : File descriptor of the open large object

## Dependencies
- Functions called/Symbols referenced:
  - [lo_initialize](lo_initialize.md)
  - PQfn
  - [PQclear](../P/PQclear.md)
  - [PQresultStatus](../P/PQresultStatus.md)
  - [lo_ntoh64](lo_ntoh64.md)
  - [libpq_append_conn_error](libpq_append_conn_error.md)
- Types referenced:
  - pg_int64
  - PQArgBlock
  - PGresult
  - PGRES_COMMAND_OK
- Called from (representative examples):
  - [pickout](../p/pickout.md) (in testlo64.c test program)
  - Client applications requiring large object support > 2GB

## Notes and Other Information
- Returns -1 on error, including when  is not available on the server
- Supports large objects up to theoretical 64-bit size limits
- Includes specific error checking for function availability on older PostgreSQL servers
- Performs network byte order conversion for proper cross-platform 64-bit integer handling
- [Result](../R/Result.md) length must be exactly 8 bytes for the operation to be considered successful
- Part of PostgreSQL's extended large object interface for handling very large objects