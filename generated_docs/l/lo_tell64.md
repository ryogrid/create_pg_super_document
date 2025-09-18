# lo_tell64

## Location
src/interfaces/libpq/fe-lobj.c: 548 - 588

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
  - lo_initialize
  - PQfn
  - PQclear
  - PQresultStatus
  - lo_ntoh64
  - libpq_append_conn_error
- Types referenced:
  - pg_int64
  - PQArgBlock
  - PGresult
  - PGRES_COMMAND_OK
- Called from (representative examples):
  - pickout (in testlo64.c test program)
  - Client applications requiring large object support > 2GB

## Notes and Other Information
- Returns -1 on error, including when  is not available on the server
- Supports large objects up to theoretical 64-bit size limits
- Includes specific error checking for function availability on older PostgreSQL servers
- Performs network byte order conversion for proper cross-platform 64-bit integer handling
- Result length must be exactly 8 bytes for the operation to be considered successful
- Part of PostgreSQL's extended large object interface for handling very large objects