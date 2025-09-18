# libpqsrv_connect_prepare

## Location
src/include/libpq/libpq-be-fe-helpers.h: 131 - 159

## Overview
Internal helper function that reserves a file descriptor before attempting PostgreSQL connection establishment.

## Definition
```c
static inline void libpqsrv_connect_prepare(void)
```

## Detailed Description
libpqsrv_connect_prepare is a critical preparation step in the libpqsrv connection establishment process. It enforces PostgreSQL's file descriptor management policies by reserving an external file descriptor before attempting to create a database connection. The function ensures compliance with fd.c's limits on non-virtual file descriptors by treating each PGconn as representing one long-lived file descriptor.

The reservation process also triggers closure of Virtual File Descriptors (VFDs) if necessary to make room for the new connection. If file descriptor acquisition fails, the function throws a detailed error with platform-specific hints for resolution, distinguishing between Unix/Linux systems (where ulimit settings matter) and Windows systems (where only PostgreSQL's configuration applies).

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - AcquireExternalFD
- Called from (representative examples):
  - [libpqsrv_connect](libpqsrv_connect.md)
  - [libpqsrv_connect_params](libpqsrv_connect_params.md)

## Notes and Other Information
- This is a static inline function defined in src/include/libpq/libpq-be-fe-helpers.h:131-159
- Part of the internal helper functions section of the libpqsrv suite
- Throws ERROR-level exceptions if file descriptor acquisition fails, preventing connection attempts
- Provides platform-specific error messages and hints (different for Windows vs Unix-like systems)
- The function assumes each PostgreSQL connection will consume exactly one long-lived file descriptor
- Must be called before any PQconnectStart* function calls in the connection establishment sequence