# pg_cancel_conn

## Location
src/interfaces/libpq/fe-cancel.c: 31 - 39

## Overview
The pg_cancel_conn structure serves as a wrapper around a PGconn to send query cancellations using PQcancelBlocking and PQcancelStart functions, providing type safety by preventing accidental interchange with regular PGconn pointers.

## Definition


## Detailed Description
The pg_cancel_conn structure is the backing struct for the opaque PGcancelConn type defined in libpq-fe.h. It acts as a specialized wrapper around a standard PGconn connection object specifically designed for query cancellation operations. The structure is intentionally not just a typedef to ensure compiler-enforced type safety - the compiler will generate errors if a PGconn is passed to a function that expects a PGcancelConn, and vice versa. This design prevents programming errors where regular connection operations might be accidentally attempted on a cancellation connection or cancellation operations on a regular connection.

## Parameters / Member Variables
- : The underlying PGconn connection object that provides the actual connection infrastructure for sending cancellation requests

## Dependencies
- Functions called/Symbols referenced:
  - PGconn (embedded as member)
- Called from (representative examples):
  - PGcancelConn (typedef in libpq-fe.h)
  - [PQcancelCreate](../P/PQcancelCreate.md) (function that creates instances)

## Notes and Other Information
- This structure is internal to libpq and not exposed to client applications
- The contents are accessed through the opaque PGcancelConn typedef
- Used in conjunction with PQcancelBlocking() and PQcancelStart()/PQcancelPoll() APIs
- Provides type safety to distinguish cancellation connections from regular database connections
- Part of PostgreSQL's secure query cancellation mechanism introduced for better connection management