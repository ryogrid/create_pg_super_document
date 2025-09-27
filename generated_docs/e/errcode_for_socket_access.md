# errcode_for_socket_access

## Location
[src/backend/utils/error/elog.c:953-988](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L953-L988)

## Overview
Maps system errno values to appropriate SQL state error codes specifically for socket access operations in PostgreSQL's error reporting system.

## Definition
```c
int errcode_for_socket_access(void)
```

## Detailed Description
This function automatically sets the SQLSTATE error code for the current error based on the saved errno value from a failed socket operation. Unlike its file access counterpart, this function has a simpler mapping scheme focused primarily on connection-related failures. It maps socket-specific error conditions to PostgreSQL-specific SQL state codes, providing standardized error reporting for network operations within the database system.

## Parameters / Member Variables
- Return value: Always returns 0 (return value is not meaningful)

## Dependencies
- Functions called/Symbols referenced:
  - [ErrorData](../E/ErrorData.md) (struct type for error information)
  - CHECK_STACK_DEPTH (macro for stack depth validation)
  - ALL_CONNECTION_FAILURE_ERRNOS (macro defining connection failure errno values)
- Called from (representative examples):
  - [ident_inet](../i/ident_inet.md) (authentication function)
  - [be_tls_open_server](../b/be_tls_open_server.md) (TLS server setup)
  - [ListenServerPort](../L/ListenServerPort.md) (server port listening)
  - [AcceptConnection](../A/AcceptConnection.md) (connection acceptance)
  - [pq_recvbuf](../p/pq_recvbuf.md) (packet receiving)
  - [SysLoggerMain](../S/SysLoggerMain.md) (system logger)

## Notes and Other Information
- The primary error message string should generally include %m when this function is used
- Simple mapping scheme for socket errors:
  - ALL_CONNECTION_FAILURE_ERRNOS → ERRCODE_CONNECTION_FAILURE
  - All other errors → ERRCODE_INTERNAL_ERROR
- Does not increment recursion depth counter
- Widely used throughout PostgreSQL's networking code, particularly in libpq and postmaster components
- Located in src/backend/utils/error/elog.c:953-988

## Simplified Source

```c
// Simplified version of errcode_for_socket_access
int errcode_for_socket_access(void) {
    // Get current error data from error stack
    ErrorData *edata = &errordata[errordata_stack_depth];

    // Validate error stack depth
    CHECK_STACK_DEPTH();

    // Map socket errno to appropriate SQL error code
    switch (edata->saved_errno) {
        // Connection failure errors (network issues)
        case ALL_CONNECTION_FAILURE_ERRNOS:
            edata->sqlerrcode = ERRCODE_CONNECTION_FAILURE;
            break;

        // All other socket errors are internal errors
        default:
            edata->sqlerrcode = ERRCODE_INTERNAL_ERROR;
            break;
    }

    // Return value is meaningless - function works by side effect
    return 0;
}
```

Key simplifications made:
- Added descriptive comments for each major step
- Clarified that the function works by modifying error data structure
- Explained the two-category error mapping approach
- Made the side-effect nature of the function explicit
- Simplified the switch statement structure for clarity