# errcode_for_socket_access

## Location
src/backend/utils/error/elog.c: 953 - 988

## Overview
Maps system errno values to appropriate SQL state error codes specifically for socket access operations in PostgreSQL's error reporting system.

## Definition
```c
int errcode_for_socket_access(void)
```

## Detailed Description
This function automatically sets the SQLSTATE error code for the current error based on the saved errno value from a failed socket operation. Unlike its file access counterpart, this function has a simpler mapping scheme focused primarily on connection-related failures. It maps socket-specific error conditions to PostgreSQL-specific SQL state codes, providing standardized error reporting for network operations within the database system.

## Parameters / Member Variables
- No parameters (void function)
- Return value: Always returns 0 (return value is not meaningful)

## Dependencies
- Functions called/Symbols referenced:
  - ErrorData (struct type for error information)
  - CHECK_STACK_DEPTH (macro for stack depth validation)
  - ALL_CONNECTION_FAILURE_ERRNOS (macro defining connection failure errno values)
- Called from (representative examples):
  - ident_inet (authentication function)
  - be_tls_open_server (TLS server setup)
  - ListenServerPort (server port listening)
  - AcceptConnection (connection acceptance)
  - pq_recvbuf (packet receiving)
  - SysLoggerMain (system logger)

## Notes and Other Information
- The primary error message string should generally include %m when this function is used
- Simple mapping scheme for socket errors:
  - ALL_CONNECTION_FAILURE_ERRNOS → ERRCODE_CONNECTION_FAILURE
  - All other errors → ERRCODE_INTERNAL_ERROR
- Does not increment recursion depth counter
- Widely used throughout PostgreSQL's networking code, particularly in libpq and postmaster components
- Located in src/backend/utils/error/elog.c:953-988